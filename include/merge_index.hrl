-record(segment,{root,
                 offsets_table,
                 size}).
-type segment() :: #segment{}.
-type segments() :: [segment()].


