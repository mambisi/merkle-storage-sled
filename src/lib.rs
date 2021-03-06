#![forbid(unsafe_code)]
#![feature(const_fn)]

mod hash;
mod blake2b;
mod base58;
mod schema;
mod codec;
mod  merkle_storage;
mod database;
mod db_iterator;

pub mod prelude {
    pub use crate::database::*;
    pub use crate::merkle_storage::*;
    pub use crate::db_iterator::*;
    pub use crate::codec::*;
}



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
