.PHONY: deps

all: deps compile

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean 
	./rebar delete-deps

eunit:
	./rebar skip_deps=true eunit

dialyzer: compile
	./rebar dialyze

docs:
	./rebar skip_deps=true doc

