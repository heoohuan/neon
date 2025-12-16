//! This module contains logic for decoding and interpreting
//! raw bytes which represent a raw Postgres WAL record.

use std::collections::HashMap;

use bytes::{Buf, Bytes};
use pageserver_api::key::rel_block_to_key;
use pageserver_api::reltag::{RelTag, SlruKind};
use pageserver_api::shard::ShardIdentity;
use postgres_ffi::walrecord::*;
use postgres_ffi::{PgMajorVersion, pg_constants};
use postgres_ffi::waldecoder::WalFormat;
use postgres_ffi_types::forknum::VISIBILITYMAP_FORKNUM;
use utils::lsn::Lsn;

use crate::models::*;
use crate::serialized_batch::SerializedValueBatch;

impl InterpretedWalRecord {
    /// Decode and interpreted raw bytes which represent one Postgres WAL record.
    /// Data blocks which do not match any of the provided shard identities are filtered out.
    /// Shard 0 is a special case since it tracks all relation sizes. We only give it
    /// the keys that are being written as that is enough for updating relation sizes.
    pub fn from_bytes_filtered(
        buf: Bytes,
        shards: &[ShardIdentity],
        next_record_lsn: Lsn,
        pg_version: PgMajorVersion,
        wal_format: WalFormat,
    ) -> anyhow::Result<HashMap<ShardIdentity, InterpretedWalRecord>> {
        let mut decoded = DecodedWALRecord::default();
        decode_wal_record(buf, &mut decoded, pg_version)?;
        let xid = decoded.xl_xid;

        let flush_uncommitted = if decoded.is_dbase_create_copy(pg_version) {
            FlushUncommittedRecords::Yes
        } else {
            FlushUncommittedRecords::No
        };

        let mut shard_records: HashMap<ShardIdentity, InterpretedWalRecord> =
            HashMap::with_capacity(shards.len());
        for shard in shards {
            shard_records.insert(
                *shard,
                InterpretedWalRecord {
                    metadata_record: None,
                    batch: SerializedValueBatch::default(),
                    next_record_lsn,
                    flush_uncommitted,
                    xid,
                },
            );
        }

        MetadataRecord::from_decoded_filtered(
            &decoded,
            &mut shard_records,
            next_record_lsn,
            pg_version,
            wal_format,
        )?;
        SerializedValueBatch::from_decoded_filtered(
            decoded,
            &mut shard_records,
            next_record_lsn,
            pg_version,
            wal_format,
        )?;

        Ok(shard_records)
    }
}

impl MetadataRecord {
    /// Populates the given `shard_records` with metadata records from this WAL record, if any,
    /// discarding those belonging to other shards.
    ///
    /// Only metadata records relevant for the given shards is emitted. Currently, most metadata
    /// records are broadcast to all shards for simplicity, but this should be improved.
    fn from_decoded_filtered(
        decoded: &DecodedWALRecord,
        shard_records: &mut HashMap<ShardIdentity, InterpretedWalRecord>,
        next_record_lsn: Lsn,
        pg_version: PgMajorVersion,
        wal_format: WalFormat,
    ) -> anyhow::Result<()> {
        // Note: this doesn't actually copy the bytes since
        // the [`Bytes`] type implements it via a level of indirection.
        let mut buf = decoded.record.clone();
        buf.advance(decoded.main_data_offset);

        // First, generate metadata records from the decoded WAL record.
        let metadata_record = match decoded.xl_rmid {
            pg_constants::RM_HEAP_ID | pg_constants::RM_HEAP2_ID => {
                Self::decode_heapam_record(&mut buf, decoded, pg_version)?
            }
            pg_constants::RM_NEON_ID => Self::decode_neonmgr_record(&mut buf, decoded, pg_version)?,
            // Handle other special record types
            pg_constants::RM_SMGR_ID => Self::decode_smgr_record(&mut buf, decoded)?,
            pg_constants::RM_DBASE_ID => Self::decode_dbase_record(&mut buf, decoded, pg_version)?,
            pg_constants::RM_TBLSPC_ID => {
                tracing::trace!("XLOG_TBLSPC_CREATE/DROP is not handled yet");
                None
            }
            pg_constants::RM_CLOG_ID => Self::decode_clog_record(&mut buf, decoded, pg_version, wal_format)?,
            pg_constants::RM_XACT_ID => {
                Self::decode_xact_record(&mut buf, decoded, next_record_lsn, wal_format)?
            }
            pg_constants::RM_MULTIXACT_ID => {
                Self::decode_multixact_record(&mut buf, decoded, pg_version)?
            }
            pg_constants::RM_RELMAP_ID => Self::decode_relmap_record(&mut buf, decoded)?,
            // This is an odd duck. It needs to go to all shards.
            // Since it uses the checkpoint image (that's initialized from CHECKPOINT_KEY
            // in WalIngest::new), we have to send the whole DecodedWalRecord::record to
            // the pageserver and decode it there.
            //
            // Alternatively, one can make the checkpoint part of the subscription protocol
            // to the pageserver. This should work fine, but can be done at a later point.
            pg_constants::RM_XLOG_ID => {
                Self::decode_xlog_record(&mut buf, decoded, next_record_lsn)?
            }
            pg_constants::RM_LOGICALMSG_ID => {
                Self::decode_logical_message_record(&mut buf, decoded)?
            }
            pg_constants::RM_STANDBY_ID => Self::decode_standby_record(&mut buf, decoded)?,
            pg_constants::RM_REPLORIGIN_ID => Self::decode_replorigin_record(&mut buf, decoded)?,
            // openGauss specific RMGRs
            pg_constants::RM_SLOT_ID => {
                tracing::trace!("RM_SLOT_ID record not handled yet");
                None
            }
            pg_constants::RM_HEAP3_ID => {
                tracing::trace!("RM_HEAP3_ID record not handled yet");
                None
            }
            pg_constants::RM_BARRIER_ID => {
                tracing::trace!("RM_BARRIER_ID record not handled yet");
                None
            }
            pg_constants::RM_MOT_ID => {
                tracing::trace!("RM_MOT_ID (Memory-Optimized Table) record not handled yet");
                None
            }
            pg_constants::RM_UHEAP_ID => Self::decode_uheap_record(&mut buf, decoded, pg_version)?,
            pg_constants::RM_UHEAP2_ID => Self::decode_uheap2_record(&mut buf, decoded, pg_version)?,
            pg_constants::RM_UNDOLOG_ID => Self::decode_undolog_record(&mut buf, decoded, pg_version)?,
            pg_constants::RM_UHEAPUNDO_ID => Self::decode_uheapundo_record(&mut buf, decoded, pg_version)?,
            pg_constants::RM_UNDOACTION_ID => Self::decode_undoaction_record(&mut buf, decoded, pg_version)?,
            pg_constants::RM_UBTREE_ID => Self::decode_ubtree_record(&mut buf, decoded, pg_version)?,
            pg_constants::RM_UBTREE2_ID => Self::decode_ubtree2_record(&mut buf, decoded, pg_version)?,
            pg_constants::RM_SEGPAGE_ID => Self::decode_segpage_record(&mut buf, decoded, pg_version)?,
            pg_constants::RM_COMPRESSION_REL_ID => {
                tracing::trace!("RM_COMPRESSION_REL_ID record not handled yet");
                None
            }
            pg_constants::RM_LOGICALDDLMSG_ID => {
                tracing::trace!("RM_LOGICALDDLMSG_ID record not handled yet");
                None
            }
            pg_constants::RM_GENERIC_ID => {
                tracing::trace!("RM_GENERIC_ID record not handled yet");
                None
            }
            pg_constants::RM_UBTREE3_ID => Self::decode_ubtree3_record(&mut buf, decoded, pg_version)?,
            pg_constants::RM_UBTREE4_ID => Self::decode_ubtree4_record(&mut buf, decoded, pg_version)?,
            _unexpected => {
                // TODO: consider failing here instead of blindly doing something without
                // understanding the protocol
                None
            }
        };

        // Next, filter the metadata record by shard.
        for (shard, record) in shard_records.iter_mut() {
            match metadata_record {
                Some(
                    MetadataRecord::Heapam(HeapamRecord::ClearVmBits(ref clear_vm_bits))
                    | MetadataRecord::Neonrmgr(NeonrmgrRecord::ClearVmBits(ref clear_vm_bits)),
                ) => {
                    // Route VM page updates to the shards that own them. VM pages are stored in the VM fork
                    // of the main relation. These are sharded and managed just like regular relation pages.
                    // See: https://github.com/neondatabase/neon/issues/9855
                    let is_local_vm_page = |heap_blk| {
                        let vm_blk = pg_constants::HEAPBLK_TO_MAPBLOCK(heap_blk);
                        shard.is_key_local(&rel_block_to_key(clear_vm_bits.vm_rel, vm_blk))
                    };
                    // Send the old and new VM page updates to their respective shards.
                    let updated_old_heap_blkno = clear_vm_bits
                        .old_heap_blkno
                        .filter(|&blkno| is_local_vm_page(blkno));
                    let updated_new_heap_blkno = clear_vm_bits
                        .new_heap_blkno
                        .filter(|&blkno| is_local_vm_page(blkno));
                    // If neither VM page belongs to this shard, discard the record.
                    if updated_old_heap_blkno.is_some() || updated_new_heap_blkno.is_some() {
                        // Clone the record and update it for the current shard.
                        let mut for_shard = metadata_record.clone();
                        match for_shard {
                            Some(
                                MetadataRecord::Heapam(HeapamRecord::ClearVmBits(
                                    ref mut clear_vm_bits,
                                ))
                                | MetadataRecord::Neonrmgr(NeonrmgrRecord::ClearVmBits(
                                    ref mut clear_vm_bits,
                                )),
                            ) => {
                                clear_vm_bits.old_heap_blkno = updated_old_heap_blkno;
                                clear_vm_bits.new_heap_blkno = updated_new_heap_blkno;
                                record.metadata_record = for_shard;
                            }
                            _ => {
                                unreachable!("for_shard is a clone of what we checked above")
                            }
                        }
                    }
                }
                Some(MetadataRecord::LogicalMessage(LogicalMessageRecord::Put(_))) => {
                    // Filter LogicalMessage records (AUX files) to only be stored on shard zero
                    if shard.is_shard_zero() {
                        record.metadata_record = metadata_record;
                        // No other shards should receive this record, so we stop traversing shards early.
                        break;
                    }
                }
                _ => {
                    // All other metadata records are sent to all shards.
                    record.metadata_record = metadata_record.clone();
                }
            }
        }

        Ok(())
    }

    fn decode_heapam_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // Handle VM bit updates that are implicitly part of heap records.

        // First, look at the record to determine which VM bits need
        // to be cleared. If either of these variables is set, we
        // need to clear the corresponding bits in the visibility map.
        let mut new_heap_blkno: Option<u32> = None;
        let mut old_heap_blkno: Option<u32> = None;
        let mut flags = pg_constants::VISIBILITYMAP_VALID_BITS;

        match pg_version {
            PgMajorVersion::PG14 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v14::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v14::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v14::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v14::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v14::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v14::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    anyhow::bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
            PgMajorVersion::PG15 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v15::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v15::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v15::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v15::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v15::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v15::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    anyhow::bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
            PgMajorVersion::PG16 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v16::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v16::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v16::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v16::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v16::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v16::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    anyhow::bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
            PgMajorVersion::PG17 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v17::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v17::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v17::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v17::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v17::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v17::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    anyhow::bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
        }

        if new_heap_blkno.is_some() || old_heap_blkno.is_some() {
            let vm_rel = RelTag {
                forknum: VISIBILITYMAP_FORKNUM,
                spcnode: decoded.blocks[0].rnode_spcnode,
                dbnode: decoded.blocks[0].rnode_dbnode,
                relnode: decoded.blocks[0].rnode_relnode,
            };

            Ok(Some(MetadataRecord::Heapam(HeapamRecord::ClearVmBits(
                ClearVmBits {
                    new_heap_blkno,
                    old_heap_blkno,
                    vm_rel,
                    flags,
                },
            ))))
        } else {
            Ok(None)
        }
    }

    fn decode_neonmgr_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // Handle VM bit updates that are implicitly part of heap records.

        // First, look at the record to determine which VM bits need
        // to be cleared. If either of these variables is set, we
        // need to clear the corresponding bits in the visibility map.
        let mut new_heap_blkno: Option<u32> = None;
        let mut old_heap_blkno: Option<u32> = None;
        let mut flags = pg_constants::VISIBILITYMAP_VALID_BITS;

        assert_eq!(decoded.xl_rmid, pg_constants::RM_NEON_ID);

        match pg_version {
            PgMajorVersion::PG16 | PgMajorVersion::PG17 => {
                let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                match info {
                    pg_constants::XLOG_NEON_HEAP_INSERT => {
                        let xlrec = v17::rm_neon::XlNeonHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_DELETE => {
                        let xlrec = v17::rm_neon::XlNeonHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_UPDATE
                    | pg_constants::XLOG_NEON_HEAP_HOT_UPDATE => {
                        let xlrec = v17::rm_neon::XlNeonHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_MULTI_INSERT => {
                        let xlrec = v17::rm_neon::XlNeonHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_LOCK => {
                        let xlrec = v17::rm_neon::XlNeonHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                    info => anyhow::bail!("Unknown WAL record type for Neon RMGR: {}", info),
                }
            }
            PgMajorVersion::PG15 | PgMajorVersion::PG14 => anyhow::bail!(
                "Neon RMGR has no known compatibility with PostgreSQL version {}",
                pg_version
            ),
        }

        if new_heap_blkno.is_some() || old_heap_blkno.is_some() {
            let vm_rel = RelTag {
                forknum: VISIBILITYMAP_FORKNUM,
                spcnode: decoded.blocks[0].rnode_spcnode,
                dbnode: decoded.blocks[0].rnode_dbnode,
                relnode: decoded.blocks[0].rnode_relnode,
            };

            Ok(Some(MetadataRecord::Neonrmgr(NeonrmgrRecord::ClearVmBits(
                ClearVmBits {
                    new_heap_blkno,
                    old_heap_blkno,
                    vm_rel,
                    flags,
                },
            ))))
        } else {
            Ok(None)
        }
    }

    fn decode_smgr_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_SMGR_CREATE {
            let create = XlSmgrCreate::decode(buf);
            let rel = RelTag {
                spcnode: create.rnode.spcnode,
                dbnode: create.rnode.dbnode,
                relnode: create.rnode.relnode,
                forknum: create.forknum,
            };

            return Ok(Some(MetadataRecord::Smgr(SmgrRecord::Create(SmgrCreate {
                rel,
            }))));
        } else if info == pg_constants::XLOG_SMGR_TRUNCATE {
            let truncate = XlSmgrTruncate::decode(buf);
            return Ok(Some(MetadataRecord::Smgr(SmgrRecord::Truncate(truncate))));
        }

        Ok(None)
    }

    fn decode_dbase_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // TODO: Refactor this to avoid the duplication between postgres versions.

        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        tracing::debug!(%info, %pg_version, "handle RM_DBASE_ID");

        match pg_version {
            PgMajorVersion::PG14 => {
                if info == postgres_ffi::v14::bindings::XLOG_DBASE_CREATE {
                    let createdb = XlCreateDatabase::decode(buf);
                    tracing::debug!("XLOG_DBASE_CREATE v14");

                    let record = MetadataRecord::Dbase(DbaseRecord::Create(DbaseCreate {
                        db_id: createdb.db_id,
                        tablespace_id: createdb.tablespace_id,
                        src_db_id: createdb.src_db_id,
                        src_tablespace_id: createdb.src_tablespace_id,
                    }));

                    return Ok(Some(record));
                } else if info == postgres_ffi::v14::bindings::XLOG_DBASE_DROP {
                    let dropdb = XlDropDatabase::decode(buf);

                    let record = MetadataRecord::Dbase(DbaseRecord::Drop(DbaseDrop {
                        db_id: dropdb.db_id,
                        tablespace_ids: dropdb.tablespace_ids,
                    }));

                    return Ok(Some(record));
                }
            }
            PgMajorVersion::PG15 => {
                if info == postgres_ffi::v15::bindings::XLOG_DBASE_CREATE_WAL_LOG {
                    tracing::debug!("XLOG_DBASE_CREATE_WAL_LOG: noop");
                } else if info == postgres_ffi::v15::bindings::XLOG_DBASE_CREATE_FILE_COPY {
                    // The XLOG record was renamed between v14 and v15,
                    // but the record format is the same.
                    // So we can reuse XlCreateDatabase here.
                    tracing::debug!("XLOG_DBASE_CREATE_FILE_COPY");

                    let createdb = XlCreateDatabase::decode(buf);
                    let record = MetadataRecord::Dbase(DbaseRecord::Create(DbaseCreate {
                        db_id: createdb.db_id,
                        tablespace_id: createdb.tablespace_id,
                        src_db_id: createdb.src_db_id,
                        src_tablespace_id: createdb.src_tablespace_id,
                    }));

                    return Ok(Some(record));
                } else if info == postgres_ffi::v15::bindings::XLOG_DBASE_DROP {
                    let dropdb = XlDropDatabase::decode(buf);
                    let record = MetadataRecord::Dbase(DbaseRecord::Drop(DbaseDrop {
                        db_id: dropdb.db_id,
                        tablespace_ids: dropdb.tablespace_ids,
                    }));

                    return Ok(Some(record));
                }
            }
            PgMajorVersion::PG16 => {
                if info == postgres_ffi::v16::bindings::XLOG_DBASE_CREATE_WAL_LOG {
                    tracing::debug!("XLOG_DBASE_CREATE_WAL_LOG: noop");
                } else if info == postgres_ffi::v16::bindings::XLOG_DBASE_CREATE_FILE_COPY {
                    // The XLOG record was renamed between v14 and v15,
                    // but the record format is the same.
                    // So we can reuse XlCreateDatabase here.
                    tracing::debug!("XLOG_DBASE_CREATE_FILE_COPY");

                    let createdb = XlCreateDatabase::decode(buf);
                    let record = MetadataRecord::Dbase(DbaseRecord::Create(DbaseCreate {
                        db_id: createdb.db_id,
                        tablespace_id: createdb.tablespace_id,
                        src_db_id: createdb.src_db_id,
                        src_tablespace_id: createdb.src_tablespace_id,
                    }));

                    return Ok(Some(record));
                } else if info == postgres_ffi::v16::bindings::XLOG_DBASE_DROP {
                    let dropdb = XlDropDatabase::decode(buf);
                    let record = MetadataRecord::Dbase(DbaseRecord::Drop(DbaseDrop {
                        db_id: dropdb.db_id,
                        tablespace_ids: dropdb.tablespace_ids,
                    }));

                    return Ok(Some(record));
                }
            }
            PgMajorVersion::PG17 => {
                if info == postgres_ffi::v17::bindings::XLOG_DBASE_CREATE_WAL_LOG {
                    tracing::debug!("XLOG_DBASE_CREATE_WAL_LOG: noop");
                } else if info == postgres_ffi::v17::bindings::XLOG_DBASE_CREATE_FILE_COPY {
                    // The XLOG record was renamed between v14 and v15,
                    // but the record format is the same.
                    // So we can reuse XlCreateDatabase here.
                    tracing::debug!("XLOG_DBASE_CREATE_FILE_COPY");

                    let createdb = XlCreateDatabase::decode(buf);
                    let record = MetadataRecord::Dbase(DbaseRecord::Create(DbaseCreate {
                        db_id: createdb.db_id,
                        tablespace_id: createdb.tablespace_id,
                        src_db_id: createdb.src_db_id,
                        src_tablespace_id: createdb.src_tablespace_id,
                    }));

                    return Ok(Some(record));
                } else if info == postgres_ffi::v17::bindings::XLOG_DBASE_DROP {
                    let dropdb = XlDropDatabase::decode(buf);
                    let record = MetadataRecord::Dbase(DbaseRecord::Drop(DbaseDrop {
                        db_id: dropdb.db_id,
                        tablespace_ids: dropdb.tablespace_ids,
                    }));

                    return Ok(Some(record));
                }
            }
        }

        Ok(None)
    }

    fn decode_clog_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        pg_version: PgMajorVersion,
        _wal_format: WalFormat,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & !pg_constants::XLR_INFO_MASK;

        // Handle OpenGauss-specific layout differences if needed
        match _wal_format {
            WalFormat::OpenGauss => {
                if info == pg_constants::CLOG_ZEROPAGE {
                    // openGauss encodes pageno as int64 in the DDL zero-page record
                    let pageno_i64 = buf.get_i64_le();
                    let pageno = pageno_i64 as u64 as u32;
                    let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

                    Ok(Some(MetadataRecord::Clog(ClogRecord::ZeroPage(
                        ClogZeroPage { segno, rpageno },
                    ))))
                } else {
                    // Treat truncate record as (pageno:int64, oldest_xid:u32, oldest_xid_db:u32)
                    let pageno_i64 = buf.get_i64_le();
                    let pageno = pageno_i64 as u64 as u32;
                    let oldest_xid = buf.get_u32_le();
                    let oldest_xid_db = buf.get_u32_le();

                    Ok(Some(MetadataRecord::Clog(ClogRecord::Truncate(
                        ClogTruncate {
                            pageno,
                            oldest_xid,
                            oldest_xid_db,
                        },
                    ))))
                }
            }
            _ => {
                if info == pg_constants::CLOG_ZEROPAGE {
                    let pageno = if pg_version < PgMajorVersion::PG17 {
                        buf.get_u32_le()
                    } else {
                        buf.get_u64_le() as u32
                    };
                    let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

                    Ok(Some(MetadataRecord::Clog(ClogRecord::ZeroPage(
                        ClogZeroPage { segno, rpageno },
                    ))))
                } else {
                    assert_eq!(info, pg_constants::CLOG_TRUNCATE);
                    let xlrec = XlClogTruncate::decode(buf, pg_version);

                    Ok(Some(MetadataRecord::Clog(ClogRecord::Truncate(
                        ClogTruncate {
                            pageno: xlrec.pageno,
                            oldest_xid: xlrec.oldest_xid,
                            oldest_xid_db: xlrec.oldest_xid_db,
                        },
                    ))))
                }
            }
        }
    }

    fn decode_xact_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        lsn: Lsn,
        _wal_format: WalFormat,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLOG_XACT_OPMASK;
        let origin_id = decoded.origin_id;
        let xl_xid = decoded.xl_xid;

        if info == pg_constants::XLOG_XACT_COMMIT {
            let parsed = XlXactParsedRecord::decode(buf, decoded.xl_xid, decoded.xl_info);
            return Ok(Some(MetadataRecord::Xact(XactRecord::Commit(XactCommon {
                parsed,
                origin_id,
                xl_xid,
                lsn,
            }))));
        } else if info == pg_constants::XLOG_XACT_ABORT {
            let parsed = XlXactParsedRecord::decode(buf, decoded.xl_xid, decoded.xl_info);
            return Ok(Some(MetadataRecord::Xact(XactRecord::Abort(XactCommon {
                parsed,
                origin_id,
                xl_xid,
                lsn,
            }))));
        } else if info == pg_constants::XLOG_XACT_COMMIT_PREPARED {
            let parsed = XlXactParsedRecord::decode(buf, decoded.xl_xid, decoded.xl_info);
            return Ok(Some(MetadataRecord::Xact(XactRecord::CommitPrepared(
                XactCommon {
                    parsed,
                    origin_id,
                    xl_xid,
                    lsn,
                },
            ))));
        } else if info == pg_constants::XLOG_XACT_ABORT_PREPARED {
            let parsed = XlXactParsedRecord::decode(buf, decoded.xl_xid, decoded.xl_info);
            return Ok(Some(MetadataRecord::Xact(XactRecord::AbortPrepared(
                XactCommon {
                    parsed,
                    origin_id,
                    xl_xid,
                    lsn,
                },
            ))));
        } else if info == pg_constants::XLOG_XACT_PREPARE {
            return Ok(Some(MetadataRecord::Xact(XactRecord::Prepare(
                XactPrepare {
                    xl_xid: decoded.xl_xid,
                    data: Bytes::copy_from_slice(&buf[..]),
                },
            ))));
        }

        Ok(None)
    }

    fn decode_multixact_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

        if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE
            || info == pg_constants::XLOG_MULTIXACT_ZERO_MEM_PAGE
        {
            let pageno = if pg_version < PgMajorVersion::PG17 {
                buf.get_u32_le()
            } else {
                buf.get_u64_le() as u32
            };
            let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
            let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

            let slru_kind = match info {
                pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE => SlruKind::MultiXactOffsets,
                pg_constants::XLOG_MULTIXACT_ZERO_MEM_PAGE => SlruKind::MultiXactMembers,
                _ => unreachable!(),
            };

            return Ok(Some(MetadataRecord::MultiXact(MultiXactRecord::ZeroPage(
                MultiXactZeroPage {
                    slru_kind,
                    segno,
                    rpageno,
                },
            ))));
        } else if info == pg_constants::XLOG_MULTIXACT_CREATE_ID {
            let xlrec = XlMultiXactCreate::decode(buf);
            return Ok(Some(MetadataRecord::MultiXact(MultiXactRecord::Create(
                xlrec,
            ))));
        } else if info == pg_constants::XLOG_MULTIXACT_TRUNCATE_ID {
            let xlrec = XlMultiXactTruncate::decode(buf);
            return Ok(Some(MetadataRecord::MultiXact(MultiXactRecord::Truncate(
                xlrec,
            ))));
        }

        Ok(None)
    }

    fn decode_relmap_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let update = XlRelmapUpdate::decode(buf);

        let mut buf = decoded.record.clone();
        buf.advance(decoded.main_data_offset);
        // skip xl_relmap_update
        buf.advance(12);

        Ok(Some(MetadataRecord::Relmap(RelmapRecord::Update(
            RelmapUpdate {
                update,
                buf: Bytes::copy_from_slice(&buf[..]),
            },
        ))))
    }

    fn decode_xlog_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        lsn: Lsn,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        Ok(Some(MetadataRecord::Xlog(XlogRecord::Raw(RawXlogRecord {
            info,
            lsn,
            buf: buf.clone(),
        }))))
    }

    fn decode_logical_message_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_LOGICAL_MESSAGE {
            let xlrec = XlLogicalMessage::decode(buf);
            let prefix = std::str::from_utf8(&buf[0..xlrec.prefix_size - 1])?;

            #[cfg(feature = "testing")]
            if prefix == "neon-test" {
                return Ok(Some(MetadataRecord::LogicalMessage(
                    LogicalMessageRecord::Failpoint,
                )));
            }

            if let Some(path) = prefix.strip_prefix("neon-file:") {
                let buf_size = xlrec.prefix_size + xlrec.message_size;
                let buf = Bytes::copy_from_slice(&buf[xlrec.prefix_size..buf_size]);
                return Ok(Some(MetadataRecord::LogicalMessage(
                    LogicalMessageRecord::Put(PutLogicalMessage {
                        path: path.to_string(),
                        buf,
                    }),
                )));
            }
        }

        Ok(None)
    }

    fn decode_standby_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_RUNNING_XACTS {
            let xlrec = XlRunningXacts::decode(buf);
            return Ok(Some(MetadataRecord::Standby(StandbyRecord::RunningXacts(
                StandbyRunningXacts {
                    oldest_running_xid: xlrec.oldest_running_xid,
                },
            ))));
        }

        Ok(None)
    }

    fn decode_replorigin_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_REPLORIGIN_SET {
            let xlrec = XlReploriginSet::decode(buf);
            return Ok(Some(MetadataRecord::Replorigin(ReploriginRecord::Set(
                xlrec,
            ))));
        } else if info == pg_constants::XLOG_REPLORIGIN_DROP {
            let xlrec = XlReploriginDrop::decode(buf);
            return Ok(Some(MetadataRecord::Replorigin(ReploriginRecord::Drop(
                xlrec,
            ))));
        }

    fn decode_uheap_insert(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapInsert: offnum (2), flags (1), padding (1) = 4 bytes
        if buf.remaining() < 4 {
            bail!("UHeap insert record too short");
        }

        let offnum = buf.get_u16_le();
        let flags = buf.get_u8();
        let _padding = buf.get_u8();

        // Check if record contains tuple data
        let has_tuple = (decoded.xl_info & 0x80) != 0; // XLOG_UHEAP_INIT_PAGE
        let tuple_data = if has_tuple && buf.remaining() > 0 {
            Some(buf.clone())
        } else {
            None
        };

        Ok(Some(MetadataRecord::UHeap(UHeapRecord::Insert(
            UHeapInsertRecord {
                offnum,
                flags,
                has_tuple,
                tuple_data,
            },
        ))))
    }

    fn decode_uheap_delete(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapDelete: oldxid (4), offnum (2), td_id (1), flag (1), padding (3) = 12 bytes
        if buf.remaining() < 12 {
            bail!("UHeap delete record too short");
        }

        let oldxid = buf.get_u32_le();
        let offnum = buf.get_u16_le();
        let td_id = buf.get_u8();
        let flag = buf.get_u8();
        let _padding = buf.get_u24_le(); // 3 bytes padding

        // Check if undo tuple is present
        let has_undo_tuple = (flag & 0x02) != 0; // XLZ_HAS_DELETE_UNDOTUPLE
        let undo_tuple = if has_undo_tuple && buf.remaining() > 0 {
            Some(buf.clone())
        } else {
            None
        };

        Ok(Some(MetadataRecord::UHeap(UHeapRecord::Delete(
            UHeapDeleteRecord {
                oldxid,
                offnum,
                td_id,
                flag,
                has_undo_tuple,
                undo_tuple,
            },
        ))))
    }

    fn decode_uheap_update(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapUpdate: oldxid (4), old_offnum (2), old_tuple_flag (2), new_offnum (2),
        // old_tuple_td_id (1), flags (1) = 12 bytes
        if buf.remaining() < 12 {
            bail!("UHeap update record too short");
        }

        let oldxid = buf.get_u32_le();
        let old_offnum = buf.get_u16_le();
        let old_tuple_flag = buf.get_u16_le();
        let new_offnum = buf.get_u16_le();
        let old_tuple_td_id = buf.get_u8();
        let flags = buf.get_u8();

        // Check for tuple data based on flags
        let has_old_tuple = (flags & 0x01) != 0; // XLZ_UPDATE_PREFIX_FROM_OLD
        let has_new_tuple = (decoded.xl_info & 0x80) != 0; // XLOG_UHEAP_INIT_PAGE

        let old_tuple = if has_old_tuple && buf.remaining() > 0 {
            Some(buf.clone())
        } else {
            None
        };

        let new_tuple = if has_new_tuple && buf.remaining() > 0 {
            Some(buf.clone())
        } else {
            None
        };

        Ok(Some(MetadataRecord::UHeap(UHeapRecord::Update(
            UHeapUpdateRecord {
                oldxid,
                old_offnum,
                old_tuple_flag,
                new_offnum,
                old_tuple_td_id,
                flags,
                has_old_tuple,
                has_new_tuple,
                old_tuple,
                new_tuple,
            },
        ))))
    }

    fn decode_uheap_freeze_td_slot(
        buf: &mut Bytes,
        _decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapFreezeTdSlot: latestFrozenXid (4), nFrozen (2), padding (6) = 12 bytes
        if buf.remaining() < 12 {
            bail!("UHeap freeze td slot record too short");
        }

        let latest_frozen_xid = buf.get_u32_le();
        let n_frozen = buf.get_u16_le();
        let _padding = buf.get_u48_le(); // 6 bytes padding

        Ok(Some(MetadataRecord::UHeap(UHeapRecord::FreezeTdSlot(
            UHeapFreezeTdSlotRecord {
                latest_frozen_xid,
                n_frozen,
            },
        ))))
    }

    fn decode_uheap_invalid_td_slot(
        buf: &mut Bytes,
        _decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // Placeholder implementation - need to determine actual structure
        let data = buf.clone();
        Ok(Some(MetadataRecord::UHeap(UHeapRecord::InvalidTdSlot(
            UHeapInvalidTdSlotRecord { data },
        ))))
    }

    fn decode_uheap_clean(
        buf: &mut Bytes,
        _decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapClean: latestRemovedXid (4), ndeleted (2), ndead (2), flags (1), padding (3) = 12 bytes
        if buf.remaining() < 12 {
            bail!("UHeap clean record too short");
        }

        let latest_removed_xid = buf.get_u32_le();
        let ndeleted = buf.get_u16_le();
        let ndead = buf.get_u16_le();
        let flags = buf.get_u8();
        let _padding = buf.get_u24_le(); // 3 bytes padding

        // Read offset numbers - 2*nredirected + ndead + nunused
        let mut offsets = Vec::new();
        while buf.remaining() >= 2 {
            offsets.push(buf.get_u16_le());
        }

        Ok(Some(MetadataRecord::UHeap(UHeapRecord::Clean(
            UHeapCleanRecord {
                latest_removed_xid,
                ndeleted,
                ndead,
                flags,
                offsets,
            },
        ))))
    }

    fn decode_uheap_multi_insert(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapMultiInsert: ntuples (4), flags (1) = 5 bytes
        if buf.remaining() < 5 {
            bail!("UHeap multi insert record too short");
        }

        let ntuples = buf.get_i32_le();
        let flags = buf.get_u8();

        let mut tuples = Vec::new();
        for _ in 0..ntuples {
            if buf.remaining() < 12 { // SizeOfMultiInsertUTuple
                bail!("UHeap multi insert tuple data too short");
            }

            let datalen = buf.get_i32_le();
            let xid = buf.get_u16_le();
            let td_id_locker_td_id = buf.get_u16_le();
            let td_id = (td_id_locker_td_id & 0xFF) as u8;
            let locker_td_id = ((td_id_locker_td_id >> 8) & 0xFF) as u8;
            let flag = buf.get_u16_le();
            let flag2 = buf.get_u16_le();
            let t_hoff = buf.get_u8();

            if buf.remaining() < datalen as usize {
                bail!("UHeap multi insert tuple data length mismatch");
            }

            let data = buf.split_to(datalen as usize);

            tuples.push(UHeapTupleData {
                datalen,
                xid,
                td_id,
                locker_td_id,
                flag,
                flag2,
                t_hoff,
                data: data.into(),
            });
        }

        Ok(Some(MetadataRecord::UHeap(UHeapRecord::MultiInsert(
            UHeapMultiInsertRecord {
                ntuples,
                flags,
                tuples,
            },
        ))))
    }

    fn decode_uheap_new_page(
        buf: &mut Bytes,
        _decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // Placeholder implementation - need to determine actual structure
        let data = buf.clone();
        Ok(Some(MetadataRecord::UHeap(UHeapRecord::NewPage(
            UHeapNewPageRecord { data },
        ))))
    }

    fn decode_uheap_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        _pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        let op = decoded.xl_info & 0x70; // XLOG_UHEAP_OPMASK

        match op {
            0x00 => {
                // XLOG_UHEAP_INSERT
                Self::decode_uheap_insert(buf, decoded)
            }
            0x10 => {
                // XLOG_UHEAP_DELETE
                Self::decode_uheap_delete(buf, decoded)
            }
            0x20 => {
                // XLOG_UHEAP_UPDATE
                Self::decode_uheap_update(buf, decoded)
            }
            0x30 => {
                // XLOG_UHEAP_FREEZE_TD_SLOT
                Self::decode_uheap_freeze_td_slot(buf, decoded)
            }
            0x40 => {
                // XLOG_UHEAP_INVALID_TD_SLOT
                Self::decode_uheap_invalid_td_slot(buf, decoded)
            }
            0x50 => {
                // XLOG_UHEAP_CLEAN
                Self::decode_uheap_clean(buf, decoded)
            }
            0x60 => {
                // XLOG_UHEAP_MULTI_INSERT
                Self::decode_uheap_multi_insert(buf, decoded)
            }
            0x70 => {
                // XLOG_UHEAP_NEW_PAGE
                Self::decode_uheap_new_page(buf, decoded)
            }
            _ => {
                // Unknown operation, return generic record
                let remaining = buf.clone();
                Ok(Some(MetadataRecord::UHeap(UHeapRecord::Generic(
                    UHeapGenericRecord {
                        info,
                        buf: remaining,
                    },
                ))))
            }
        }
    }

    fn decode_uheap2_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        _pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        let op = decoded.xl_info & 0x30; // UHeap2 operation mask

        match op {
            0x00 => {
                // XLOG_UHEAP2_BASE_SHIFT
                Self::decode_uheap2_base_shift(buf, decoded)
            }
            0x10 => {
                // XLOG_UHEAP2_FREEZE
                Self::decode_uheap2_freeze(buf, decoded)
            }
            0x20 => {
                // XLOG_UHEAP2_EXTEND_TD_SLOTS
                Self::decode_uheap2_extend_td_slots(buf, decoded)
            }
            _ => {
                // Unknown operation, return generic record
                let remaining = buf.clone();
                Ok(Some(MetadataRecord::UHeap2(UHeap2Record::Generic(
                    UHeap2GenericRecord {
                        info,
                        buf: remaining,
                    },
                ))))
            }
        }
    }

    fn decode_uheap2_base_shift(
        buf: &mut Bytes,
        _decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapBaseShift: multi (1), delta (8) = 9 bytes
        if buf.remaining() < 9 {
            bail!("UHeap2 base shift record too short");
        }

        let multi = buf.get_u8() != 0;
        let delta = buf.get_i64_le();

        Ok(Some(MetadataRecord::UHeap2(UHeap2Record::BaseShift(
            UHeap2BaseShiftRecord { multi, delta },
        ))))
    }

    fn decode_uheap2_freeze(
        buf: &mut Bytes,
        _decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapFreeze: cutoff_xid (4) = 4 bytes
        if buf.remaining() < 4 {
            bail!("UHeap2 freeze record too short");
        }

        let cutoff_xid = buf.get_u32_le();

        Ok(Some(MetadataRecord::UHeap2(UHeap2Record::Freeze(
            UHeap2FreezeRecord { cutoff_xid },
        ))))
    }

    fn decode_uheap2_extend_td_slots(
        buf: &mut Bytes,
        _decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapExtendTdSlots: nPrevSlots (1), nExtended (1) = 2 bytes
        if buf.remaining() < 2 {
            bail!("UHeap2 extend td slots record too short");
        }

        let n_prev_slots = buf.get_u8();
        let n_extended = buf.get_u8();

        Ok(Some(MetadataRecord::UHeap2(UHeap2Record::ExtendTdSlots(
            UHeap2ExtendTdSlotsRecord {
                n_prev_slots,
                n_extended,
            },
        ))))
    }
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        let remaining = buf.clone();
        Ok(Some(MetadataRecord::UndoLog(UndoLogRecord::Generic(
            UndoLogGenericRecord {
                info,
                buf: remaining,
            },
        ))))
    }

    fn decode_uheapundo_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        _pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        let op = decoded.xl_info & 0x30; // UHeapUndo operation mask

        match op {
            0x00 => {
                // XLOG_UHEAPUNDO_PAGE
                Self::decode_uheapundo_page(buf, decoded)
            }
            0x10 => {
                // XLOG_UHEAPUNDO_RESET_SLOT
                Self::decode_uheapundo_reset_slot(buf, decoded)
            }
            0x20 => {
                // XLOG_UHEAPUNDO_ABORT_SPECINSERT
                Self::decode_uheapundo_abort_specinsert(buf, decoded)
            }
            _ => {
                // Unknown operation, return generic record
                let remaining = buf.clone();
                Ok(Some(MetadataRecord::UHeapUndo(UHeapUndoRecord::Generic(
                    UHeapUndoGenericRecord {
                        info,
                        buf: remaining,
                    },
                ))))
            }
        }
    }

    fn decode_uheapundo_page(
        buf: &mut Bytes,
        _decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // Placeholder implementation - UHeapUndoActionWALInfo structure
        let data = buf.clone();
        Ok(Some(MetadataRecord::UHeapUndo(UHeapUndoRecord::Page(
            UHeapUndoPageRecord { data },
        ))))
    }

    fn decode_uheapundo_reset_slot(
        buf: &mut Bytes,
        _decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapUndoResetSlot: urec_ptr (8), zone_id (4), td_slot_id (4) = 16 bytes
        if buf.remaining() < 16 {
            bail!("UHeap undo reset slot record too short");
        }

        let urec_ptr = buf.get_u64_le();
        let zone_id = buf.get_i32_le();
        let td_slot_id = buf.get_i32_le();

        Ok(Some(MetadataRecord::UHeapUndo(UHeapUndoRecord::ResetSlot(
            UHeapUndoResetSlotRecord {
                urec_ptr,
                zone_id,
                td_slot_id,
            },
        ))))
    }

    fn decode_uheapundo_abort_specinsert(
        buf: &mut Bytes,
        _decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // XlUHeapUndoAbortSpecInsert: offset (2), zone_id (4) = 6 bytes
        if buf.remaining() < 6 {
            bail!("UHeap undo abort specinsert record too short");
        }

        let offset = buf.get_u16_le();
        let zone_id = buf.get_i32_le();

        Ok(Some(MetadataRecord::UHeapUndo(UHeapUndoRecord::AbortSpecInsert(
            UHeapUndoAbortSpecInsertRecord { offset, zone_id },
        ))))
    }

    fn decode_undoaction_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        _pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // Undo action records
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        let remaining = buf.clone();
        Ok(Some(MetadataRecord::UndoAction(UndoActionRecord::Generic(
            UndoActionGenericRecord {
                info,
                buf: remaining,
            },
        ))))
    }

    fn decode_ubtree_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        _pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // UHeap BTree index records
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        let remaining = buf.clone();
        Ok(Some(MetadataRecord::UHeapBtree(UHeapBtreeRecord::Generic(
            UHeapBtreeGenericRecord {
                info,
                buf: remaining,
            },
        ))))
    }

    fn decode_ubtree2_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        _pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // UHeap BTree2 index records
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        let remaining = buf.clone();
        Ok(Some(MetadataRecord::UHeapBtree2(UHeapBtree2Record::Generic(
            UHeapBtree2GenericRecord {
                info,
                buf: remaining,
            },
        ))))
    }

    fn decode_segpage_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        _pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // Segment page storage records
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        let remaining = buf.clone();
        Ok(Some(MetadataRecord::Segpage(SegpageRecord::Generic(
            SegpageGenericRecord {
                info,
                buf: remaining,
            },
        ))))
    }

    fn decode_ubtree3_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        _pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // UHeap BTree3 index records
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        let remaining = buf.clone();
        Ok(Some(MetadataRecord::UHeapBtree3(UHeapBtree3Record::Generic(
            UHeapBtree3GenericRecord {
                info,
                buf: remaining,
            },
        ))))
    }

    fn decode_ubtree4_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        _pg_version: PgMajorVersion,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // UHeap BTree4 index records
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        let remaining = buf.clone();
        Ok(Some(MetadataRecord::UHeapBtree4(UHeapBtree4Record::Generic(
            UHeapBtree4GenericRecord {
                info,
                buf: remaining,
            },
        ))))
    }
}
