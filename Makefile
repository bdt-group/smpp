REBAR ?= rebar3
PROJECT := smpp

.PHONY: compile clean distclean xref dialyzer dialyze linter lint test

all: compile

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

distclean: clean
	rm -rf _build

xref:
	@$(REBAR) xref

dialyzer:
	@$(REBAR) dialyzer

dialyze:
	@$(REBAR) dialyzer

linter:
	@$(REBAR) as lint lint

lint:
	@$(REBAR) as lint lint

test:
	@$(REBAR) do eunit, ct
