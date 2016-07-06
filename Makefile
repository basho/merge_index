.PHONY: deps test xref dialyzer

all: deps compile

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean 
	./rebar delete-deps

test:
	./rebar skip_deps=true eunit

docs:
	./rebar skip_deps=true doc

