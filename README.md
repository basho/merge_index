# Overview

MergeIndex is an Erlang library for storing ordered sets on
disk. It is very similar to an SSTable (in Google's
Bigtable) or an HFile (in Hadoop).

Basho Technologies developed MergeIndex to serve as the underlying
index storage format for Riak Search and the upcoming Secondary Index
functionality in Riak.

MergeIndex has the following characteristics:

* Fast write performance; handles "spiky" situations with high write
  loads gracefully.
* Fast read performance for single lookups.
* Moderately fast read performance for range queries. 
* "Immutable" data files, so the system can be stopped hard with no ill effects.
* Timestamp-based conflict resolution.
* Relatively low RAM usage.
* Stores data in compressed form.
  
And some tradeoffs:

* Range queries can cause spiky RAM usage, depending on range size.
* Needs a high number of file handles. During extremely high write
  loads for extended periods of time, this can exhaust file handles
  and ETS tables if the system is not given a chance to recover.
* High disk churn during segment compaction. 


# Data Model

A MergeIndex database is a three-level hierarchy of data. (The
chosen terminology reflects merge_index's roots as a storage engine
for document data, but it can be considered a general purpose index.)

The hierarchy is:

* **Index** - Top level. For example: `shoes`. 
* **Field** - Second level. For example: `color`.
* **Term** - Third level. For example: `red`.

Underneath each term, you can store one or more values, with
associated properties and a timestamp:

* **Value** - The value to store, usually. In indexing situations, this
  may be a primary key or document ID. For example: "SKU-52167"

* **Properties** - Additional metadata associated with the value,
  returned in lookups and range queries. Setting properties to
  `undefined` will delete a value from the database.

* **Timestamp** - A user-defined timestamp value. This is used by the
  system to resolve conflicts. The largest timestamp wins.

These six fields together form a **Posting**. For example:

    {Index, Field, Term, Value, Properties, Timestamp}
    {<<"shoes">>, <<"color">>, <<"red">>, <<"SKU-52167">>, [], 23487197}

# API

* `merge_index:start_link(DataDir)` - Open a MergeIndex
  database. Note that the database is NOT thread safe, and this is NOT
  enforced by the software. You should only have one Pid per
  directory, otherwise your data will be corrupted.
  
* `merge_index:index(Pid, Index, Field, Term, Value, Properties,
  Timestamp)` - Index a posting.
  
* `merge_index:index(Pid, Postings)` - Index a list of postings.

* `merge_index:lookup(Pid, Index, Field, Term)` - Returns an iterator
  that will yield all of the `{Value, Properties}` records stored
  under the provided term. The iterator should be consumed quickly, as
  the results are stored in the Erlang mailbox of the calling Pid. The
  iterator is a function that when executed returns either `{Result,
  NewIterator}` or `eof`.

* `merge_index:lookup_sync(Pid, Index, Field, Term)` - Returns a list of
  `{Value, Properties}` records.
  
* `merge_index:range(Pid, Index, Field, StartTerm, EndTerm)` - Returns
  an iterator that will yield all of the `{Value, Properties}` records
  matching the provided range, inclusive. The iterator should be
  consumed quickly, as the results are stored in the Erlang mailbox of
  the calling Pid. The iterator is a function that when executed
  returns either `{Result, NewIterator}` or `eof`.

* `merge_index:range_sync(Pid, Index, Field, StartTerm, EndTerm)` -
  Returns a list of the `{Value, Properties}` records matching the
  provided range, inclusive. The iterator should be consumed quickly,
  as the results are stored in the Erlang mailbox of the calling Pid.

* `merge_index:info(Pid, Index, Field, Term)` - Get an *estimate* of
  how many results exist for a given term. This is an estimate
  because, for performance reasons, the system does not factor
  tombstones into the result. In addition, due to how bloom filters
  and signatures are used, results may be miscounted. This is mainly
  meant to be used as a way to optimize query planning, not for
  reliable counts.

* `merge_index:compact(Pid)` - Trigger a compaction, if necessary.

* `merge_index:drop(Pid)` - Delete a MergeIndex database. This will
  delete your data.
  
* `merge_index:stop(Pid)` - Close a merge_index database.

# Example Usage

The example below opens a merge_index database, generates some dummy
data using a list comprehension, indexes the dummy data, and then
performs a lookup and range query.

    %% Open a merge_index database.
    application:start(merge_index),
    {ok, Pid} = merge_index:start_link("./merge_index_data"),
     
    %% Index a posting...
    merge_index:index(Pid, "index", "field", "term", "value1", [], 1),
     
    %% Run a query, get results back as a list...
    List1 = merge_index:lookup_sync(Pid, "index", "field", "term"),
    io:format("lookup_sync1:~n~p~n", [List1]),
     
    %% Run a query, get results back as an iterator. 
    %% Iterator returns {Result, NewIterator} or 'eof'.
    Iterator1 = merge_index:lookup(Pid, "index", "field", "term"),
    {Result1, Iterator2} = Iterator1(),
    eof = Iterator2(),
    io:format("lookup:~n~p~n", [Result1]),
     
    %% Index multiple postings...
    merge_index:index(Pid, [
        {"index", "field", "term", "value1", [], 2},
        {"index", "field", "term", "value2", [], 2},
        {"index", "field", "term", "value3", [], 2}
    ]),
     
    %% Run another query...
    List2 = merge_index:lookup_sync(Pid, "index", "field", "term"),
    io:format("lookup_sync2:~n~p~n", [List2]),
     
    %% Delete some postings...
    merge_index:index(Pid, [
        {"index", "field", "term", "value1", undefined, 3},
        {"index", "field", "term", "value3", undefined, 3}
    ]),
     
    %% Run another query...
    List3 = merge_index:lookup_sync(Pid, "index", "field", "term"),
    io:format("lookup_sync2:~n~p~n", [List3]),
     
    %% Delete the database.
    merge_index:drop(Pid),
     
    %% Close the database.
    merge_index:stop(Pid).

# Architecture

At a high level, MergeIndex is a collection of one or more
in-memory **buffers** storing recently written data, plus one or more
immutable **segments** storing older data. As data is written, the
buffers are converted to segments, and small segments are compacted
together to form larger segments. Each buffer is backed by an
append-only disk log, ensuring that the buffer state is recoverable if
the system is shut down before the buffer is converted to a segment.

Queries involve all active buffers and segments, but avoid touching
disk as much as possible. Queries against buffers execute directly
against memory, and as a result are fast. Queries against segments
consult an in-memory offsets table with a bloom filter and signature
table to determine key existence, and then seek directly to the
correct disk position if the key is found within a given segment. If
the key is not found, there is no disk penalty.

## MI Server (`mi_server` module)

The mi\_server module holds the coordinating logic of MergeIndex. It
keeps track of which buffers and segments exist, handles incoming
writes, manages locks on buffers and segments, and spawns new
processes to respond to queries.

During startup, mi_server performs the following steps:

* Delete any files that should be deleted, marked by a file of the
  same name with a ".deleted" extension.
* Load the latest buffer (determined by buffer number)
  as the active buffer.
* Convert any other buffers to segments.
* Open all segment offset files.

It then waits for incoming **index**, **lookup**, or **range**
requests.

On an **index** request, `mi_server` is passed a list of postings, of
the form `{Index, Field, Term, Value, Props, Timestamp}`. As a speed
optimization, we invert the timestamp (multiply by -1). This allows a
simple ascending sort to put the latest timestamped value first
(otherwise the earliest timestamped value would be first). Later,
iterators across data used during querying and compacting take
advantage of this information to filter out duplicates. Also, each
posting is translated to `{{Index, Field, Term}, Value,
InvertedTimestamp, Props}` which is the posting format that the buffer
expects. The postings are then written to the buffer. If the index
operation causes the buffer to exceed the `buffer_rollover_size`
setting, then the buffer is converted to a segment. More details are
in the following sections.

On a **lookup** request, `mi_server` is passed an Index, Field, and
Term. It first puts a lock on all buffers and segments that will be
used in the query. This ensures that buffers and segments won't be
deleted before the query has completed. Next, it spawns a linked
process that creates an iterator across each buffer and segment for
the provided Index/Field/Term key, and returns the results in
ascending sorted order.

A **range** request is similar to a **lookup** request, except the
iterators return the values for a range of keys.

## Buffers (`mi_buffer` module)

A buffer consists of an in-memory Erlang ETS table plus an append only
log file. All new data written to a MergeIndex database is first
written to the buffer. Once a buffer reaches a certain size, it is
converted to a segment.

MergeIndex opens the ETS table as a `duplicate_bag`, keyed on `{Index, Field,
Term}`. Postings are written to the buffer in a batch. 

At query time, the MergeIndex performs an `ets:lookup/N` to retrieve
matching postings, sorts them, and wraps them in an iterator.

Range queries work slightly differently. MergeIndex gets a list of
keys from the table, filters the keys according to what matches the
range, and then returns an iterator for each key. 

Buffer contents are also stored on disk in an append-only log file,
named `buffer.<NUMBER>`. The format is simple: a 4-byte unsigned
integer followed by the `term_to_binary/1` encoded bytes for the list
of postings.

When a buffer exceeds `buffer_rollover_size`, it is converted to a
segment. The system puts the contents of the ETS table into a list,
sorts the list, constructs an iterator over the list, and then sends
the iterator to the same process used to compact segments, described
below.

## Segments (`mi_segment` module)

A segment consists of a **data file** and an **offsets table**. It is
immutable; once written, it is read only. (Though eventually it may be
compacted into a larger segment and deleted.)

The **data file** is a flat file with the following format: a key, followed
by a list of values, followed by another key, followed by another list
of values. Both the keys an the values are sorted in ascending
order. Conceptually, the data file is split into blocks (approximately
32k in size by default). The offsets table contains one entry per block.

A **key** is an `{Index, Field, Term}` tuple. To save space, if the key
has the same `Index` as the previous key, and it is not the first key
in a block, then the Index will be omitted from the tuple. Likewise
with the `Field`. The key is stored as a single bit set to '1',
followed by a 15-bit unsigned integer containing the size of the
key on disk, followed by the `term_to_binary/N` representation of the
key. The maximum on-disk key size is 32k.

A **value** is a `{Value, Timestamp, Props}` tuple. It is put in this
order to optimize sorting and comparisons during later
operations. The list of values is compressed, and then stored as a
single bit set to '0', followed by a 31-bit unsigned integer
containing the size of the list of values on disk, followed by the
`term_to_binary/N` representation of the values. If the list of values
is larger than the `segment_values_compression_threshold`, then the
values are compressed. If the list of values grows larger than the
`segment_values_staging_size`, then it is broken up into multiple
chunks. The maximum on-disk value size is theoretically 2GB.

The **offsets table** is an ETS ordered_set table with an entry for
each block in the **data file**. The entry is keyed on the *last* key
in the block (which makes lookup using `ets:next/2` possible).

Each entry is a compressed tuple containing:

* The offset of the block in the data file. (Variable length integer.)
* A bloom filter on the keys contained in the block. (200 bytes)
* The longest prefix contained by all keys in the block. For example,
  the prefix for the keys "business", "bust", "busy" would be
  "bus". This is currently unused. (Size depends on keys.)
* A list of entries for each key containing two signatures, plus some
  lookup information allowing the system to skip directly to the
  proper read location during queries. Each entry is approximately 5 bytes,
  but could be up to 10 bytes for very large values.
  * An edit signature, constructed by comparing the current term
    against the final term in the block. The edit signature is a
    bitstring, where the bit is '0' if the bytes match, or '1' if the
    bytes don't match.
  * A hash signature, which is a single byte representation of the
    bytes of the term xor'd and rotated.
  * The size of the key on disk.
  * The size of the values on disk.
  * The total number of values.

## Compaction (`mi_segment_writer` module)

When the number of segments passes a threshold, the system compacts
segments. This merges together the data files from multiple segments
to create a new, larger segment, and deletes the old segments. In the
process, duplicate or deleted values (determined by a tombstone) are
removed. The `mi_scheduler` module ensures that only one compaction
occurs at a time on a single Erlang VM, even when multiple
MergeIndex databases are opened.

The advantage of compaction is that it moves the values for a given
key closer together on disk, and reduces the number of disk seeks
necessary to find the values for a given lookup. The disadvantage of a
compaction is that it requires the system to rewrite all of the data
involved in the compaction. 

When the system decides to perform a compaction, it focuses on the
smallest segments first. This ensures we get the optimal "bang for our
buck" out of compaction, doing the most to reduce file handle usage
and disk seeks during a query while touching the smallest amount of
data.

To perform the compaction, the `mi_server` module spawns a new linked
process. The process opens an iterator across each segment in the
compaction set. The data is stored in sorted order by key and value,
so the iterator simply needs to walk through the values from the
beginning to the end of the file. The
`segment_compact_read_ahead_size` setting determines how much of a
file cache we use when reading the segment. For small segments, it
might make sense to read the entire segment into memory, the
`segment_full_read_size` setting determines this threshold. In this
case, `segment_compact_read_ahead_size` is unused. The individual
iterators are grouped into a single master iterator.

The `mi_segment_writer` module reads values from the master iterator,
writing keys and values to the data file and offset information to the
offsets table. While writing, the segment-in-progress is marked with a
file of the same name with a ".deleted" extension, ensuring that if
the system crashes and restarts, then it will be removed. Once
finished, the obsolete segments are marked with files with ".deleted"
extensions. 

Note: That this is the same process used when rolling a single buffer
into a segment.

## Locking (`mi_locks` module)

MergeIndex uses a functional locking structure to manage
locks on buffers and segments. The locks are really a form of
reference counting. During query time, the system opens iterators
against all available buffers and segments. This increments a separate
lock count for each buffer and segment. When the query ends, the
system decrements the lock count. Once a buffer rollover (or segment
compaction) makes a buffer (or segment) obsolete, the system registers
a function to call when the lock count drops to zero. This is a simple
and easy way to make sure that buffers and segments stay around as
long as necessary to answer queries, but no longer.

New queries are directed to the latest buffers and segments, they don't
touch obsolete buffers or segments, so even during periods of high
query loads, we are guaranteed that the locks will eventually be
released and the obsolete buffers or segments deleted.

# Configuration Settings

## Overview

MergeIndex exposes a number of dials to tweak operations and RAM
usage. 

The most important MergeIndex setting in terms of memory usage is
`buffer_rollover_size`. This affects how large the buffer is allowed
to grow, in bytes, before getting converted to an on-disk
segment. The higher this number, the less frequently a MergeIndex
database will need compactions.

The second most important settings for memory usage are a combination
of `segment_full_read_size` and `max_compact_segments`. During
compaction, the system will completely page any segments smaller than
the `segment_full_read_size` value into memory. This should generally
be as large or larger than the
`buffer_rollover_size`. 

`max_compact_segments` is the maximum number of segments to compact at
one time. The higher this number, the more segments MergeIndex can
involve in each compaction. In the worst case, a compaction could take
(`segment_full_read_size` * `max_compact_segments`) bytes of RAM.

The rest of the settings have a much smaller impact on performance and
memory usage, and exist mainly for tweaking and special cases. 

## Full List of Settings

* `buffer_rollover_size` - The maximum size a buffer can reach before
  it is converted into a segment. Note that this is measured in terms
  of ETS table size, not the size the data will take on disk. Because
  of compaction, the actual segment on disk may be substantially
  smaller than this number. Default is 1MB. This setting is one of
  the biggest factors in determining how much memory MergeIndex
  will use, and how often compaction is needed. Setting this to a very
  small number (ie: 100k) will cause buffers to roll out to disk very
  rapidly, but compaction will trigger more often. Setting this to a
  very high number (ie: 10MB) will use more RAM, but require fewer
  compactions.
* `buffer_delayed_write_size` - The number of bytes the buffer log can
  write before being synced to disk. The smaller this number, the
  less chance of data data loss during a hard kill of the system,
  with the tradeoff that touching disk is expensive. This is set to a
  high number (500k) by default, so the system mainly relies on the
  `buffer_delayed_write_ms` setting to ensure crash safety.
* `buffer_delayed_write_ms` - The number of milliseconds between
  syncing the buffer log to disk. This is set to 2 seconds by default,
  meaning that the system will lose at most 2 seconds of data.
* `max_compact_segments` - The maximum number of segments to compact
  at once. Setting this to a low number means more compactions, but
  compactions will occur more quickly. The default is 20, which is a
  very high number of segments. In practice, this limit will only be
  reached during long periods of high write throughput.
* `segment_query_read_ahead_size` - The number of bytes to allocate to
  the read-ahead buffer for reading data files during a query. Default is
  65k. In practice, since we are reading compressed lists of keys from
  data files, it is okay to keep this number quite small. 
* `segment_compact_read_ahead_size` - The number of bytes to allocate
  to the read-ahead buffer for reading data files during
  compaction. During compaction, the system is reading from multiple
  file handles, and writing to a file handle. As a result, the disk
  head is skipping around quite a bit. Setting this to a large value
  can reduce the number of file seeks, reducing disk head movement,
  and speeding up compaction. Default is 5MB.
* `segment_file_buffer_size` - The amount of data to accumulate in the
  `mi_segment_writer` process between writes to disk. This acts as a
  first level of write buffering. The following two settings combined
  act as a second level of write buffering. Default is 20MB.
* `segment_delayed_write_size` - The number of bytes to allocate to the
  write buffer for writing a data file during compaction. The system
  will flush after this many bytes are written. As mentioned
  previously, the system juggles multiple file handles during a
  compaction. Setting this to a large value can substantially reduce
  disk movement. Default is 20MB.
* `segment_delayed_write_ms` - The number of milliseconds between
  syncs while writing a data file during compaction. As mentioned
  previously, the system juggles multiple file handles during a
  compaction. Setting this to a long interval can substantially reduce
  disk movement. Default is 10000. (10 seconds).
* `segment_full_read_size` - During compaction, segments below this
  size are read completely into memory. This can help reduce disk
  contention during compaction, at the expense of using more
  RAM. Default is 5MB.
* `segment_block_size` - Determines the size of each block in a
  segment data file. Since there is one offset table entry per block,
  this indirectly determines how many entries are in the offsets
  table. Each offsets table has an overhead of ~200 bytes, plus about
  5 bytes per key, so fewer offset entries means means less RAM
  usage. The tradeoff is that too few offset entries may cause false
  positive key lookups during a query, leading to wasted disk
  seeks. Default is 32k.
* `segment_values_staging_size` - The number of values that
  `mi_segment_writer` should accumulate before writing values to a
  segment data file. Default is 1000.
* `segment_values_compression_threshold` - Determines the point at
  which the segment writer begins compressing the staging list. If the
  list of values to write is greater than the threshold, then the data
  is compressed before storing to disk. Compression is a tradeoff
  between time/CPU usage and space , so raising this value can reduce
  load on the CPU at the cost of writing more data to disk. Default is
  0.
* `segment_values_compression_level` - Determines the compression
  level to use when compressing data. Default is 1. Valid values are 1
  through 9.

A number of configuration settings are fuzzed: 

* `buffer_rollover_size` by 25%
* `buffer_delayed_write_size` by 10%
* `buffer_delayed_write_ms` by 10%

"Fuzzed" means that the actual value is increased or decreased by a
certain random percent. If you open multiple MergeIndex databases
and write to them with an evenly balanced load, then all of the
buffers tend to roll over at the same time. Fuzzing spaces out the
rollovers.

# Troubleshooting

## Determine the Number of Open Buffers/Segments

Run the following command to check how many buffers are currently
open:

    find <PATH> -name "buffer.*" | wc -l
    
Run the following command to check how many segments are currently
open:

    find <PATH> -name "segment.*.data" | wc -l
    
Run the following command to determine whether a compaction is
currently in progress:

    find <PATH> -name "segment.*.data.deleted"

## Check Memory Usage

Run the following code in the Erlang shell to see how much space the
in-memory buffers are consuming:

    WordSize = erlang:system_info(wordsize),
    F = fun(X, Acc) -> 
        case ets:info(X, name) == 'buffer' of
            true -> Acc + (ets:info(X, memory) * WordSize);
            false -> Acc
        end
    end,
    lists:foldl(F, 0, ets:all()).
    
Run the following code in the Erlang shell to see how much space the
segment offset tables are consuming:

    WordSize = erlang:system_info(wordsize),
    F = fun(X, Acc) -> 
        case ets:info(X, name) == 'segment_offsets' of
            true -> Acc + (ets:info(X, memory) * WordSize);
            false -> Acc
        end
    end,
    lists:foldl(F, 0, ets:all()).

# Further Reading
+ [Google's Bigtable Paper - PDF](http://static.googleusercontent.com/external_content/untrusted_dlcp/labs.google.com/en/us/papers/bigtable-osdi06.pdf)
+ [HFile Description - cloudpr.blogspot.com](http://cloudepr.blogspot.com/2009/09/hfile-block-indexed-file-format-to.html)
+ [HFile Paper - slideshare.net](http://www.slideshare.net/schubertzhang/hfile-a-blockindexed-file-format-to-store-sorted-keyvalue-pairs)
