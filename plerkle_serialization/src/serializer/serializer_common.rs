use crate::Pubkey;

impl From<&[u8]> for Pubkey {
    fn from(slice: &[u8]) -> Self {
       let arr = <[u8; 32]>::try_from(slice);
        Pubkey::new(&arr.unwrap())
    }
}