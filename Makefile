.PHONY: deps test

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

dialyzer: compile
	./rebar dialyze

docs:
	./rebar skip_deps=true doc

