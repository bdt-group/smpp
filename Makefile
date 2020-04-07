REBAR ?= rebar3
PROJECT := smpp
BUILD_IMAGE  ?= gitlab.bdt.tools:5000/build-ubuntu1804:1.4.2

.PHONY: compile clean distclean xref dialyzer dialyze linter lint

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

# Build in docker environment
exec_compose_container = docker exec -i -u $(shell id -un) $(notdir ${CURDIR})_$(PROJECT)_1 $1

.PHONY: d_% dc_% compose decompose

d_%:
	./build-with-env --image $(BUILD_IMAGE) make $*

dc_%: compose
	$(call exec_compose_container,make $*)

compose: export DC_IMAGE_NAME=$(BUILD_IMAGE)
compose: export DC_UID   = $(shell id -u)
compose: export DC_GID   = $(shell id -g)
compose: export DC_USER  = $(shell id -un)
compose: export DC_GROUP = $(shell id -gn)
compose:
	docker-compose up -d

decompose: export DC_IMAGE_NAME=$(BUILD_IMAGE)
decompose: export DC_UID   = $(shell id -u)
decompose: export DC_GID   = $(shell id -g)
decompose: export DC_USER  = $(shell id -un)
decompose: export DC_GROUP = $(shell id -gn)
decompose:
	docker-compose down
