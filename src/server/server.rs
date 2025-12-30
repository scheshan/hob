use crate::{entry, Result};
use crate::arg::Args;
use crate::arrow::{ArrowRecordBatchStream, ArrowSchema};
use crate::entry::{Entry, EntryBatch};
use crate::schema::{SchemaStore, infer_schema, need_evolve_schema};
use crate::server::id::IdGenerator;
use crate::storage::manifest::{ManifestRecord, ManifestWriter};
use crate::storage::{MemTable, SSTable, SSTableKey, SSTableWriter};
use crate::stream::Stream;
use std::cmp::max;
use std::collections::HashMap;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use anyhow::anyhow;
use arrow_schema::SchemaRef;
use bytes::BytesMut;
use serde_json::Value;

#[derive(Clone)]
pub struct Server {
    args: Args,
    id_generator: IdGenerator,
    schema_store: SchemaStore,
    mem_table_id: Arc<AtomicU64>,
    ss_table_id: Arc<AtomicU64>,
    inner: Arc<RwLock<ServerInner>>,
    manifest_writer: ManifestWriter,
}

impl Server {
    pub fn new(
        id_generator: IdGenerator,
        schema_store: SchemaStore,
        manifest_writer: ManifestWriter,
        args: Args,
    ) -> Result<Self> {
        let mem_table = MemTable::new(args.root_dir.clone(), 0)?;
        manifest_writer.write(ManifestRecord::NewMemTable(mem_table.id()))?;

        Ok(Self {
            id_generator,
            schema_store,
            args,
            mem_table_id: Arc::new(AtomicU64::new(1)),
            ss_table_id: Arc::new(AtomicU64::new(1)),
            inner: Arc::new(RwLock::new(ServerInner::new(mem_table))),
            manifest_writer,
        })
    }

    pub fn recovery(id_generator: IdGenerator,
                    schema_store: SchemaStore,
                    manifest_writer: ManifestWriter,
                    args: Args,
                    recovery_state: ServerRecoveryState
    ) -> Result<Self> {
        //region recover mem_table

        let mut mem_table_list = Vec::new();
        for mem_table_id in recovery_state.mem_table_ids {
            let mem_table = MemTable::recovery(args.root_dir.clone(), mem_table_id)?;
            mem_table_list.push(mem_table);
        }

        //If the newest mem_table doesn't reach threshold size, use it as the hot mem_table
        let mem_table = if !mem_table_list.is_empty() && mem_table_list[mem_table_list.len() - 1].approximate_size() < args.mem_table_size {
            let mut mem_table = mem_table_list.remove(mem_table_list.len() - 1);
            mem_table.wal_writer().set_to_file_end()?;
            mem_table
        } else {
            let id : u64 = if mem_table_list.is_empty() {
                0
            } else {
                mem_table_list[mem_table_list.len() - 1].id() + 1
            };
            let mem_table = MemTable::new(args.root_dir.clone(), id)?;
            mem_table
        };

        let mem_table_id = Arc::new(AtomicU64::new(mem_table.id() + 1));

        //endregion

        //region recover ss_table
        let mut max_ss_table_id = 0u64;

        let mut streams = HashMap::new();
        for (stream_name, ss_table_keys) in recovery_state.stream_ss_table_keys {
            let stream = Stream::new(stream_name.clone());
            for ss_table_key in ss_table_keys {
                max_ss_table_id = max(max_ss_table_id, ss_table_key.id());
                stream.ss_table_list().push(Arc::new(SSTable::new(ss_table_key)));
            }
            streams.insert(stream_name, stream);
        }

        let ss_table_id = Arc::new(AtomicU64::new(max_ss_table_id + 1));
        //endregion

        let mut inner = ServerInner::new(mem_table);
        for im_mem_table in mem_table_list {
            inner.mem_table_list.push(Arc::new(im_mem_table));
        }
        inner.streams = streams;

        Ok(Self {
            id_generator,
            schema_store,
            args,
            mem_table_id,
            ss_table_id,
            manifest_writer,
            inner: Arc::new(RwLock::new(inner))
        })
    }

    pub fn args_ref(&self) -> &Args {
        &self.args
    }

    pub fn ingest(&self, stream_name: &String, json: Value) -> Result<()> {
        let batch = self.generate_entry_batch(stream_name, json)?;

        if batch.entries.is_empty() {
            return Ok(());
        }

        self.make_room_for_stream(stream_name);

        let mut buf = BytesMut::new();
        entry::encode_to_bytes(&mut buf, &batch);

        let mut inner = self.inner.write().unwrap();
        inner.mem_table.wal_writer().write(buf)?;
        inner.mem_table.add(stream_name, batch.schema.clone(), batch)?;

        if inner.mem_table.approximate_size() > self.args.mem_table_size {
            log::info!("Generate new mem_table");
            let new_mem_table_id = self.next_mem_table_id();
            let new_mem_table = MemTable::new(self.args.root_dir.clone(), new_mem_table_id)?;
            let manifest_record = ManifestRecord::NewMemTable(new_mem_table_id);
            self.manifest_writer.write(manifest_record)?;

            let old_mem_table = mem::replace(&mut inner.mem_table, new_mem_table);
            inner.mem_table_list.push(Arc::new(old_mem_table));
        }

        Ok(())
    }

    pub async fn flush_mem_table(&self) -> Result<()> {
        let mem_table_list = self.im_mem_table_list();

        if mem_table_list.is_empty() {
            return Ok(());
        }

        //flush ss_table
        let mut max_mem_table_id = 0;
        let mut ss_table_keys = Vec::new();

        let mut builders = HashMap::new();
        for mem_table in mem_table_list {
            for (stream_name, partitions) in mem_table.partitions() {
                for data in partitions.data() {
                    if !builders.contains_key(data.key()) {
                        builders.insert(
                            data.key().clone(),
                            ArrowRecordBatchStream::new(data.schema().clone()),
                        );
                    }
                    let builder = builders.get_mut(data.key()).unwrap();
                    for rb in data.data() {
                        builder.add_record_batch(rb.clone())?;
                    }
                }
            }
            max_mem_table_id = max(max_mem_table_id, mem_table.id());
        }

        for (key, mut builder) in builders {
            let ss_table_key = SSTableKey::new(key, self.next_ss_table_id());
            let mut writer = SSTableWriter::try_new(
                self.args.root_dir.clone(),
                ss_table_key.clone(),
                builder.arrow_schema().clone(),
            )
            .await?;
            loop {
                match builder.next_record_batch() {
                    Some(rb) => writer.write(rb).await?,
                    None => break,
                }
            }

            writer.close().await?;

            ss_table_keys.push(ss_table_key);
        }

        self.manifest_writer.write(ManifestRecord::FlushMemTable(
            max_mem_table_id,
            ss_table_keys.clone(),
        ))?;

        //replace memory data
        let mut guard = self.inner.write().unwrap();
        guard.mem_table_list = guard
            .mem_table_list
            .iter()
            .filter(|m| m.id() > max_mem_table_id)
            .map(|m| m.clone())
            .collect();

        for ss_table_key in ss_table_keys {
            let stream = guard.streams.get_mut(ss_table_key.stream_name()).unwrap();
            stream.add_ss_table(SSTable::new(ss_table_key));
        }

        Ok(())
    }

    fn populate_id(&self, batch: &mut EntryBatch) {
        let mut id_range = self.id_generator.generate_n(batch.entries.len());
        for entry in &mut batch.entries {
            entry.id = id_range.next().unwrap()
        }
    }

    fn generate_entry_batch(&self, stream_name: &String, json: Value) -> Result<EntryBatch> {
        if !json.is_array() {
            return Err(anyhow!("Invalid ingest json"))
        }

        let mut entries = Vec::new();
        for json_obj in json.as_array().unwrap() {
            let entry = Entry::try_from(json_obj)?;
            entries.push(entry);
        }

        let schema = infer_schema(&entries[0]);
        let arrow_schema = self.generate_schema(stream_name, schema)?;

        let mut batch = EntryBatch::new_with_capacity(stream_name.clone(), arrow_schema, entries.len());
        for entry in entries {
            batch.add(entry);
        }

        self.populate_id(&mut batch);

        Ok(batch)
    }

    fn generate_schema(&self, stream_name: &String, infer_schema: SchemaRef) -> Result<ArrowSchema> {
        loop {
            match self.schema_store.get(stream_name) {
                None => {
                    //if schema not exists, try to store the inferred schema to store
                    let arrow_schema = ArrowSchema::new(infer_schema.clone(), 1);
                    if self.schema_store.set(stream_name, 0, arrow_schema.clone()) {
                        return Ok(arrow_schema);
                    }
                }
                Some(exist_schema) => {
                    match need_evolve_schema(exist_schema.schema(), infer_schema.clone()) {
                        None => {
                            //if no need to evolve, return the exist schema
                            return Ok(exist_schema);
                        }
                        Some(new_schema) => {
                            //try to store the combined schema
                            let arrow_schema =
                                ArrowSchema::new(new_schema, exist_schema.version() + 1);
                            if self.schema_store.set(
                                stream_name,
                                exist_schema.version(),
                                arrow_schema.clone(),
                            ) {
                                return Ok(arrow_schema);
                            }
                        }
                    }
                }
            }
        }
    }

    fn next_mem_table_id(&self) -> u64 {
        self.mem_table_id.fetch_add(1, Ordering::Relaxed)
    }

    fn next_ss_table_id(&self) -> u64 {
        self.ss_table_id.fetch_add(1, Ordering::Relaxed)
    }

    fn im_mem_table_list(&self) -> Vec<Arc<MemTable>> {
        let guard = self.inner.read().unwrap();
        guard.mem_table_list.clone()
    }

    fn make_room_for_stream(&self, stream_name: &String) {
        let guard = self.inner.read().unwrap();
        if guard.streams.contains_key(stream_name) {
            return;
        }

        drop(guard);
        let mut guard = self.inner.write().unwrap();
        if !guard.streams.contains_key(stream_name) {
            guard
                .streams
                .insert(stream_name.clone(), Stream::new(stream_name.clone()));
        }
    }
}

struct ServerInner {
    mem_table: MemTable,
    mem_table_list: Vec<Arc<MemTable>>,
    streams: HashMap<String, Stream>,
}

impl ServerInner {
    pub fn new(mem_table: MemTable) -> Self {
        Self {
            mem_table,
            mem_table_list: Vec::new(),
            streams: HashMap::new(),
        }
    }
}

pub struct ServerRecoveryState {
    mem_table_ids: Vec<u64>,
    stream_ss_table_keys: HashMap<String, Vec<SSTableKey>>,
}

impl ServerRecoveryState {
    pub fn new(
        mem_table_ids: Vec<u64>,
        last_flush_mem_table_id: Option<u64>,
        stream_ss_table_keys: HashMap<String, Vec<SSTableKey>>,
    ) -> Self {
        let mut mem_table_ids: Vec<u64> = mem_table_ids
            .iter()
            .filter(|id| {
                last_flush_mem_table_id.is_none() || **id > last_flush_mem_table_id.unwrap()
            })
            .map(|id| *id)
            .collect();
        mem_table_ids.sort();

        Self {
            mem_table_ids,
            stream_ss_table_keys,
        }
    }
}
