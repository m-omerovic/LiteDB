﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static LiteDB.Constants;

namespace LiteDB.Engine
{
    public partial class LiteEngine
    {
        private IEnumerable<BsonDocument> SysDumpData()
        {
            var length = _dataFile.Length;
            var position = 0;

            while (position < length)
            {
                var page = _dataFile.ReadPage(position, false);

                yield return this.DumpPage(position, page);

                position += PAGE_SIZE;
            }
        }

        private IEnumerable<BsonDocument> SysDumpWal()
        {
            var length = _wal.WalFile.Length;
            var position = 0;

            while (position < length)
            {
                var page = _wal.WalFile.ReadPage(position, false);
                var doc = this.DumpPage(position, page);

                // now, add page version from WAL index
                if(_wal.Index.TryGetValue(page.PageID, out var slot))
                {
                    var version = slot.Where(x => x.Value == position).Select(x => x.Key).FirstOrDefault();

                    doc["version"] = version;
                }

                yield return doc;

                position += PAGE_SIZE;
            }
        }

        /// <summary>
        /// Dump page information into a BsonDocument
        /// </summary>
        private BsonDocument DumpPage(long position, BasePage page)
        {
            var doc = new BsonDocument
            {
                ["_position"] = (int)position,
                ["pageID"] = (int)page.PageID,
                ["pageType"] = page.PageType.ToString(),
                ["prevPageID"] = (int)page.PrevPageID,
                ["nextPageID"] = (int)page.NextPageID,
                ["itemCount"] = (int)page.ItemCount,
                ["freeBytes"] = (int)page.FreeBytes,
                ["transactionID"] = page.TransactionID.ToString()
            };

            if (page.PageType == PageType.Header)
            {
                var header = page as HeaderPage;

                doc["freeEmptyPageID"] = (int)header.FreeEmptyPageID;
                doc["lastPageID"] = (int)header.LastPageID;
                doc["creationTime"] = header.CreationTime;
                doc["lastCommit"] = header.LastCommit;
                doc["lastCheckpoint"] = header.LastCheckpoint;
                doc["lastAnalyze"] = header.LastAnalyze;
                doc["lastVaccum"] = header.LastVaccum;
                doc["lastShrink"] = header.LastShrink;
                doc["commitCounter"] = (int)header.CommitCount;
                doc["checkpointCounter"] = (int)header.CheckpointCounter;
                doc["userVersion"] = header.UserVersion;

                doc["colections"] = new BsonArray(header.Collections.Select(x => new BsonDocument
                {
                    ["name"] = x.Key,
                    ["pageID"] = (int)x.Value
                }));

            }
            else if (page.PageType == PageType.Collection)
            {
                var colPage = page as CollectionPage;

                doc["collectionName"] = colPage.CollectionName;
                doc["freeDataPageID"] = (int)colPage.FreeDataPageID;
                doc["documentCount"] = (int)colPage.DocumentCount;
                doc["sequence"] = (int)colPage.Sequence;
                doc["creationTime"] = colPage.CreationTime;
                doc["indexes"] = new BsonArray(colPage.GetIndexes(true).Select(x => new BsonDocument
                {
                    ["slot"] = x.Slot,
                    ["name"] = x.Name,
                    ["expression"] = x.Expression,
                    ["unique"] = x.Unique,
                    ["headPageID"] = (int)x.HeadNode.PageID,
                    ["maxLevel"] = (int)x.MaxLevel,
                    ["keyCount"] = (int)x.KeyCount,
                    ["uniqueKeyCount"] = (int)x.UniqueKeyCount
                }));
            }
            // all other page types contains data-only


            return doc;
        }
    }
}