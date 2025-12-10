.PHONY: all compile test eunit xref dialyzer clean distclean doc

REBAR := rebar3

APPS = erts kernel stdlib sasl crypto compiler inets mnesia public_key runtime_tools snmp syntax_tools tools xmerl
PLT_FILE = .leo_dcerl_dialyzer_plt
DOT_FILE = leo_dcerl.dot
CALL_GRAPH_FILE = leo_dcerl.png

all: compile xref eunit

compile:
	@$(REBAR) compile

test: eunit

eunit:
	@$(REBAR) eunit

xref:
	@$(REBAR) xref

check_plt:
	@$(REBAR) compile
	dialyzer --check_plt --plt $(PLT_FILE) --apps $(APPS)

build_plt:
	@$(REBAR) compile
	dialyzer --build_plt --output_plt $(PLT_FILE) --apps $(APPS)

dialyzer:
	@$(REBAR) dialyzer

typer:
	typer --plt $(PLT_FILE) -I include/ -r src/

doc:
	@$(REBAR) edoc

callgraph: graphviz
	dot -Tpng -o$(CALL_GRAPH_FILE) $(DOT_FILE)

graphviz:
	$(if $(shell which dot),,$(error "To make the depgraph, you need graphviz installed"))

clean:
	@$(REBAR) clean

distclean: clean
	@rm -rf _build
	@rm -rf priv/*.so
	@rm -rf c_src/libcutil
