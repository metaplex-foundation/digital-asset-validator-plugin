// automatically generated by the FlatBuffers compiler, do not modify

// @generated

use crate::common_generated::*;
use core::{cmp::Ordering, mem};

extern crate flatbuffers;
use self::flatbuffers::{EndianScalar, Follow};

pub enum AccountInfoOffset {}
#[derive(Copy, Clone, PartialEq, Eq)]

pub struct AccountInfo<'a> {
    pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for AccountInfo<'a> {
    type Inner = AccountInfo<'a>;
    #[inline]
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: flatbuffers::Table::new(buf, loc),
        }
    }
}

impl<'a> AccountInfo<'a> {
    pub const VT_PUBKEY: flatbuffers::VOffsetT = 4;
    pub const VT_LAMPORTS: flatbuffers::VOffsetT = 6;
    pub const VT_OWNER: flatbuffers::VOffsetT = 8;
    pub const VT_EXECUTABLE: flatbuffers::VOffsetT = 10;
    pub const VT_RENT_EPOCH: flatbuffers::VOffsetT = 12;
    pub const VT_DATA: flatbuffers::VOffsetT = 14;
    pub const VT_WRITE_VERSION: flatbuffers::VOffsetT = 16;
    pub const VT_SLOT: flatbuffers::VOffsetT = 18;
    pub const VT_IS_STARTUP: flatbuffers::VOffsetT = 20;
    pub const VT_SEEN_AT: flatbuffers::VOffsetT = 22;

    #[inline]
    pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
        AccountInfo { _tab: table }
    }
    #[allow(unused_mut)]
    pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
        _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
        args: &'args AccountInfoArgs<'args>,
    ) -> flatbuffers::WIPOffset<AccountInfo<'bldr>> {
        let mut builder = AccountInfoBuilder::new(_fbb);
        builder.add_seen_at(args.seen_at);
        builder.add_slot(args.slot);
        builder.add_write_version(args.write_version);
        builder.add_rent_epoch(args.rent_epoch);
        builder.add_lamports(args.lamports);
        if let Some(x) = args.data {
            builder.add_data(x);
        }
        if let Some(x) = args.owner {
            builder.add_owner(x);
        }
        if let Some(x) = args.pubkey {
            builder.add_pubkey(x);
        }
        builder.add_is_startup(args.is_startup);
        builder.add_executable(args.executable);
        builder.finish()
    }

    #[inline]
    pub fn pubkey(&self) -> Option<&'a Pubkey> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<Pubkey>(AccountInfo::VT_PUBKEY, None) }
    }
    #[inline]
    pub fn lamports(&self) -> u64 {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<u64>(AccountInfo::VT_LAMPORTS, Some(0))
                .unwrap()
        }
    }
    #[inline]
    pub fn owner(&self) -> Option<&'a Pubkey> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<Pubkey>(AccountInfo::VT_OWNER, None) }
    }
    #[inline]
    pub fn executable(&self) -> bool {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<bool>(AccountInfo::VT_EXECUTABLE, Some(false))
                .unwrap()
        }
    }
    #[inline]
    pub fn rent_epoch(&self) -> u64 {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<u64>(AccountInfo::VT_RENT_EPOCH, Some(0))
                .unwrap()
        }
    }
    #[inline]
    pub fn data(&self) -> Option<flatbuffers::Vector<'a, u8>> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                    AccountInfo::VT_DATA,
                    None,
                )
        }
    }
    #[inline]
    pub fn write_version(&self) -> u64 {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<u64>(AccountInfo::VT_WRITE_VERSION, Some(0))
                .unwrap()
        }
    }
    #[inline]
    pub fn slot(&self) -> u64 {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<u64>(AccountInfo::VT_SLOT, Some(0)).unwrap() }
    }
    #[inline]
    pub fn is_startup(&self) -> bool {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<bool>(AccountInfo::VT_IS_STARTUP, Some(false))
                .unwrap()
        }
    }
    #[inline]
    pub fn seen_at(&self) -> i64 {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<i64>(AccountInfo::VT_SEEN_AT, Some(0))
                .unwrap()
        }
    }
}

impl flatbuffers::Verifiable for AccountInfo<'_> {
    #[inline]
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        use self::flatbuffers::Verifiable;
        v.visit_table(pos)?
            .visit_field::<Pubkey>("pubkey", Self::VT_PUBKEY, false)?
            .visit_field::<u64>("lamports", Self::VT_LAMPORTS, false)?
            .visit_field::<Pubkey>("owner", Self::VT_OWNER, false)?
            .visit_field::<bool>("executable", Self::VT_EXECUTABLE, false)?
            .visit_field::<u64>("rent_epoch", Self::VT_RENT_EPOCH, false)?
            .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                "data",
                Self::VT_DATA,
                false,
            )?
            .visit_field::<u64>("write_version", Self::VT_WRITE_VERSION, false)?
            .visit_field::<u64>("slot", Self::VT_SLOT, false)?
            .visit_field::<bool>("is_startup", Self::VT_IS_STARTUP, false)?
            .visit_field::<i64>("seen_at", Self::VT_SEEN_AT, false)?
            .finish();
        Ok(())
    }
}
pub struct AccountInfoArgs<'a> {
    pub pubkey: Option<&'a Pubkey>,
    pub lamports: u64,
    pub owner: Option<&'a Pubkey>,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    pub write_version: u64,
    pub slot: u64,
    pub is_startup: bool,
    pub seen_at: i64,
}
impl Default for AccountInfoArgs<'_> {
    #[inline]
    fn default() -> Self {
        AccountInfoArgs {
            pubkey: None,
            lamports: 0,
            owner: None,
            executable: false,
            rent_epoch: 0,
            data: None,
            write_version: 0,
            slot: 0,
            is_startup: false,
            seen_at: 0,
        }
    }
}

pub struct AccountInfoBuilder<'a: 'b, 'b> {
    fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> AccountInfoBuilder<'a, 'b> {
    #[inline]
    pub fn add_pubkey(&mut self, pubkey: &Pubkey) {
        self.fbb_
            .push_slot_always::<&Pubkey>(AccountInfo::VT_PUBKEY, pubkey);
    }
    #[inline]
    pub fn add_lamports(&mut self, lamports: u64) {
        self.fbb_
            .push_slot::<u64>(AccountInfo::VT_LAMPORTS, lamports, 0);
    }
    #[inline]
    pub fn add_owner(&mut self, owner: &Pubkey) {
        self.fbb_
            .push_slot_always::<&Pubkey>(AccountInfo::VT_OWNER, owner);
    }
    #[inline]
    pub fn add_executable(&mut self, executable: bool) {
        self.fbb_
            .push_slot::<bool>(AccountInfo::VT_EXECUTABLE, executable, false);
    }
    #[inline]
    pub fn add_rent_epoch(&mut self, rent_epoch: u64) {
        self.fbb_
            .push_slot::<u64>(AccountInfo::VT_RENT_EPOCH, rent_epoch, 0);
    }
    #[inline]
    pub fn add_data(&mut self, data: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u8>>) {
        self.fbb_
            .push_slot_always::<flatbuffers::WIPOffset<_>>(AccountInfo::VT_DATA, data);
    }
    #[inline]
    pub fn add_write_version(&mut self, write_version: u64) {
        self.fbb_
            .push_slot::<u64>(AccountInfo::VT_WRITE_VERSION, write_version, 0);
    }
    #[inline]
    pub fn add_slot(&mut self, slot: u64) {
        self.fbb_.push_slot::<u64>(AccountInfo::VT_SLOT, slot, 0);
    }
    #[inline]
    pub fn add_is_startup(&mut self, is_startup: bool) {
        self.fbb_
            .push_slot::<bool>(AccountInfo::VT_IS_STARTUP, is_startup, false);
    }
    #[inline]
    pub fn add_seen_at(&mut self, seen_at: i64) {
        self.fbb_
            .push_slot::<i64>(AccountInfo::VT_SEEN_AT, seen_at, 0);
    }
    #[inline]
    pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> AccountInfoBuilder<'a, 'b> {
        let start = _fbb.start_table();
        AccountInfoBuilder {
            fbb_: _fbb,
            start_: start,
        }
    }
    #[inline]
    pub fn finish(self) -> flatbuffers::WIPOffset<AccountInfo<'a>> {
        let o = self.fbb_.end_table(self.start_);
        flatbuffers::WIPOffset::new(o.value())
    }
}

impl core::fmt::Debug for AccountInfo<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut ds = f.debug_struct("AccountInfo");
        ds.field("pubkey", &self.pubkey());
        ds.field("lamports", &self.lamports());
        ds.field("owner", &self.owner());
        ds.field("executable", &self.executable());
        ds.field("rent_epoch", &self.rent_epoch());
        ds.field("data", &self.data());
        ds.field("write_version", &self.write_version());
        ds.field("slot", &self.slot());
        ds.field("is_startup", &self.is_startup());
        ds.field("seen_at", &self.seen_at());
        ds.finish()
    }
}
#[inline]
/// Verifies that a buffer of bytes contains a `AccountInfo`
/// and returns it.
/// Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `root_as_account_info_unchecked`.
pub fn root_as_account_info(buf: &[u8]) -> Result<AccountInfo, flatbuffers::InvalidFlatbuffer> {
    flatbuffers::root::<AccountInfo>(buf)
}
#[inline]
/// Verifies that a buffer of bytes contains a size prefixed
/// `AccountInfo` and returns it.
/// Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `size_prefixed_root_as_account_info_unchecked`.
pub fn size_prefixed_root_as_account_info(
    buf: &[u8],
) -> Result<AccountInfo, flatbuffers::InvalidFlatbuffer> {
    flatbuffers::size_prefixed_root::<AccountInfo>(buf)
}
#[inline]
/// Verifies, with the given options, that a buffer of bytes
/// contains a `AccountInfo` and returns it.
/// Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `root_as_account_info_unchecked`.
pub fn root_as_account_info_with_opts<'b, 'o>(
    opts: &'o flatbuffers::VerifierOptions,
    buf: &'b [u8],
) -> Result<AccountInfo<'b>, flatbuffers::InvalidFlatbuffer> {
    flatbuffers::root_with_opts::<AccountInfo<'b>>(opts, buf)
}
#[inline]
/// Verifies, with the given verifier options, that a buffer of
/// bytes contains a size prefixed `AccountInfo` and returns
/// it. Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `root_as_account_info_unchecked`.
pub fn size_prefixed_root_as_account_info_with_opts<'b, 'o>(
    opts: &'o flatbuffers::VerifierOptions,
    buf: &'b [u8],
) -> Result<AccountInfo<'b>, flatbuffers::InvalidFlatbuffer> {
    flatbuffers::size_prefixed_root_with_opts::<AccountInfo<'b>>(opts, buf)
}
#[inline]
/// Assumes, without verification, that a buffer of bytes contains a AccountInfo and returns it.
/// # Safety
/// Callers must trust the given bytes do indeed contain a valid `AccountInfo`.
pub unsafe fn root_as_account_info_unchecked(buf: &[u8]) -> AccountInfo {
    flatbuffers::root_unchecked::<AccountInfo>(buf)
}
#[inline]
/// Assumes, without verification, that a buffer of bytes contains a size prefixed AccountInfo and returns it.
/// # Safety
/// Callers must trust the given bytes do indeed contain a valid size prefixed `AccountInfo`.
pub unsafe fn size_prefixed_root_as_account_info_unchecked(buf: &[u8]) -> AccountInfo {
    flatbuffers::size_prefixed_root_unchecked::<AccountInfo>(buf)
}
#[inline]
pub fn finish_account_info_buffer<'a>(
    fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    root: flatbuffers::WIPOffset<AccountInfo<'a>>,
) {
    fbb.finish(root, None);
}

#[inline]
pub fn finish_size_prefixed_account_info_buffer<'a>(
    fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    root: flatbuffers::WIPOffset<AccountInfo<'a>>,
) {
    fbb.finish_size_prefixed(root, None);
}
