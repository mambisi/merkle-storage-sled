use crate::schema::KeyValueSchema;
use crate::codec::{SchemaError, Encoder, Decoder};
use sled::{Error, Iter, IVec, Db, Batch};
use failure::Fail;
use std::marker::PhantomData;
use crate::db_iterator;
use std::collections::HashMap;
use crate::db_iterator::{DBIterator, DBIterationHandler};

impl From<SchemaError> for DBError {
    fn from(error: SchemaError) -> Self {
        DBError::SchemaError { error }
    }
}

#[derive(Debug, Fail)]
pub enum DBError {
    #[fail(display = "SledDB error: {}", error)]
    SledError {
        error: Error
    },
    #[fail(display = "Schema error: {}", error)]
    SchemaError {
        error: SchemaError
    },
}

impl From<Error> for DBError {
    fn from(error: Error) -> Self {
        DBError::SledError { error }
    }
}

impl slog::Value for DBError {
    fn serialize(&self, _record: &slog::Record, key: slog::Key, serializer: &mut dyn slog::Serializer) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self))
    }
}

pub struct DBStats {
    size_on_disk: u64
}


/// Custom trait extending RocksDB to better handle and enforce database schema
pub trait KeyValueStoreWithSchema<S: KeyValueSchema> {
    /// Insert new key value pair into the database. If key already exists, method will fail
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    /// * `value` - Value to be inserted associated with given key, specified by schema
    fn put(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

    /// Delete existing value associated with given key from the database.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    fn delete(&self, key: &S::Key) -> Result<(), DBError>;

    /// Insert key value pair into the database, overriding existing value if exists.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    /// * `value` - Value to be inserted associated with given key, specified by schema
    fn merge(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

    /// Read value associated with given key, if exists.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    fn get(&self, key: &S::Key) -> Result<Option<S::Value>, DBError>;

    /// Read all entries in database.
    ///
    /// # Arguments
    /// * `mode` - Reading mode, specified by RocksDB, From start to end, from end to start, or from
    /// arbitrary position to end.
    fn iterator(&self, mode: IteratorMode<S>) -> Result<IteratorWithSchema<S>, DBError>;

    /// Starting from given key, read all entries to the end.
    ///
    /// # Arguments
    /// * `key` - Key (specified by schema), from which to start reading entries
    fn prefix_iterator(&self, key: &S::Key) -> Result<IteratorWithSchema<S>, DBError>;

    /// Check, if database contains given key
    ///
    /// # Arguments
    /// * `key` - Key (specified by schema), to be checked for existence
    fn contains(&self, key: &S::Key) -> Result<bool, DBError>;

    /// Insert new key value pair into WriteBatch.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    /// * `value` - Value to be inserted associated with given key, specified by schema
    fn put_batch(&self, batch: &mut Batch, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

    /// Write batch into DB atomically
    ///
    /// # Arguments
    /// * `batch` - WriteBatch containing all batched writes to be written to DB
    fn write_batch(&self, batch: Batch) -> Result<(), DBError>;

    /// Get memory usage statistics from DB
    fn get_mem_use_stats(&self) -> Result<DBStats, DBError>;
}

pub struct IteratorWithSchema<'a, S: KeyValueSchema>(DBIterator<'a>, PhantomData<S>);

impl<'a, S: KeyValueSchema> Iterator for IteratorWithSchema<'a, S> {
    type Item = (Result<S::Key, SchemaError>, Result<S::Value, SchemaError>);

    fn next(&mut self) -> Option<Self::Item> {
        let i = match self.0.next() {
            None => {
                return None;
            }
            Some(d) => {
                d
            }
        };


        match i {
            Ok((k, v)) => {
                Some((S::Key::decode(&k), S::Value::decode(&v)))
            }
            Err(_) => {
                None
            }
        }
    }
}

pub struct SledDBWrapper {
    db: sled::Db
}

impl SledDBWrapper {
    pub fn new(db: sled::Db) -> Self {
        SledDBWrapper {
            db
        }
    }
}

/// Database iterator direction
pub enum Direction {
    Forward,
    Reverse,
}

/// Database iterator with schema mode, from start to end, from end to start or from specific key to end/start
pub enum IteratorMode<'a, S: KeyValueSchema> {
    Start,
    End,
    From(&'a S::Key, Direction),
}

impl<S: KeyValueSchema> KeyValueStoreWithSchema<S> for SledDBWrapper {
    fn put(&self, key: &S::Key, value: &S::Value) -> Result<(), DBError> {
        let key = key.encode()?;
        let value = value.encode()?;
        match self.db.insert(key, value) {
            Ok(_) => {
                Ok(())
            }
            Err(error) => {
                Err(DBError::SledError {
                    error
                })
            }
        }
    }

    fn delete(&self, key: &S::Key) -> Result<(), DBError> {
        let key = key.encode()?;
        match self.db.remove(key) {
            Ok(_) => {
                Ok(())
            }
            Err(error) => {
                Err(DBError::SledError {
                    error
                })
            }
        }
    }

    fn merge(&self, key: &S::Key, value: &<S as KeyValueSchema>::Value) -> Result<(), DBError> {
        let key = key.encode()?;
        let value = value.encode()?;

        match self.db.merge(&key, &value) {
            Ok(_) => {
                Ok(())
            }
            Err(error) => {
                Err(DBError::SledError {
                    error
                })
            }
        }
    }

    fn get(&self, key: &S::Key) -> Result<Option<S::Value>, DBError> {
        let key = key.encode()?;

        match self.db.get(&key) {
            Ok(v) => {
                Ok(Some(S::Value::decode(&v.unwrap_or_default())?))
            }
            Err(error) => {
                Err(DBError::SledError {
                    error
                })
            }
        }
    }

    fn iterator(&self, mode: IteratorMode<S>) -> Result<IteratorWithSchema<S>, DBError> {
        let iter = match mode {
            IteratorMode::Start => {
                self.db.iterator(db_iterator::IteratorMode::Start)
            }
            IteratorMode::End => {
                self.db.iterator(db_iterator::IteratorMode::End)
            }
            IteratorMode::From(key, direction) => {
                let key = key.encode()?;
                match direction {
                    Direction::Forward => {
                        self.db.iterator(db_iterator::IteratorMode::From(key.into(), db_iterator::Direction::Forward))
                    }
                    Direction::Reverse => {
                        self.db.iterator(db_iterator::IteratorMode::From(key.into(), db_iterator::Direction::Reverse))
                    }
                }
            }
        };
        Ok(IteratorWithSchema(iter, PhantomData))
    }

    fn prefix_iterator(&self, key: &S::Key) -> Result<IteratorWithSchema<S>, DBError> {
        let key = key.encode()?;
        let iter = self.db.scan_prefix_iterator(&key);
        Ok(IteratorWithSchema(iter, PhantomData))
    }

    fn contains(&self, key: &S::Key) -> Result<bool, DBError> {
        match self.db.contains_key(key.encode()?) {
            Ok(b) => {
                Ok(b)
            }
            Err(error) => {
                Err(DBError::SledError {
                    error
                })
            }
        }
    }

    fn put_batch(&self, batch: &mut Batch, key: &S::Key, value: &S::Value) -> Result<(), DBError> {
        let key = key.encode()?;
        let value = value.encode()?;
        batch.insert(key, value);
        Ok(())
    }

    fn write_batch(&self, batch: Batch) -> Result<(), DBError> {
        match self.db.apply_batch(batch) {
            Ok(_) => {
                Ok(())
            }
            Err(error) => {
                Err(DBError::SledError {
                    error
                })
            }
        }
    }

    fn get_mem_use_stats(&self) -> Result<DBStats, DBError> {
        Ok(DBStats {
            size_on_disk: self.db.size_on_disk().unwrap_or(0)
        })
    }
}


