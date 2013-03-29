-record(segment,{root,
                 offsets_table,
                 mtime,
                 size}).
-type segment() :: #segment{}.
-type segments() :: [segment()].


