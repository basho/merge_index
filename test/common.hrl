-define(POW_2(N), trunc(math:pow(2, N))).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).
