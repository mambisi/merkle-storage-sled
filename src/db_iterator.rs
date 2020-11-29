use sled::{Error, Iter, IVec, Db};
use crate::schema::KeyValueSchema;


/// Database iterator direction
///
#[derive(Clone)]
pub enum Direction {
    Forward,
    Reverse,
}

#[derive(Clone)]
pub enum IteratorMode{
    Start,
    End,
    From(IVec, Direction),
}

pub struct DBIterator<'a> {
    raw: &'a Db,
    mode: IteratorMode,
}

impl<'a> DBIterator<'a> {
    pub(crate) fn new(raw: &'a Db, mode: IteratorMode) -> Self {
        DBIterator {
            raw,
            mode,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl<'a> Iterator for DBIterator<'a> {
    type Item = Result<(IVec, IVec)>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.mode {
            IteratorMode::Start => {
                self.raw.iter().next()
            }
            IteratorMode::End => {
                self.raw.iter().last()
            }
            IteratorMode::From(k, direction) => {
                let key = k.to_vec();
                match direction {
                    Direction::Forward => {
                        self.raw.range(key..).next()
                    }
                    Direction::Reverse => {
                        self.raw.range(key..).last()
                    }
                }
            }
        }
    }
}

pub trait DBIterationHandler {
    fn iterator(&self, mode: IteratorMode) -> DBIterator;
    fn scan_prefix_iterator(&self, prefix: &[u8]) -> DBIterator;
}

impl DBIterationHandler for Db {
    fn iterator(&self, mode: IteratorMode) -> DBIterator {
        DBIterator::new(self, mode)
    }

    fn scan_prefix_iterator(&self, prefix: &[u8]) -> DBIterator {
        DBIterator::new(self, IteratorMode::From(IVec::from(prefix), Direction::Forward))
    }
}
