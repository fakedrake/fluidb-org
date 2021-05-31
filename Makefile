SHELL := /usr/bin/env bash
SOURCES := $(shell find "$(PWD)/src/*" -type f -name "*.hs" -print)
READDUMP_EXE := /tmp/readdump
LOG_DUMP := /tmp/benchmark.out
BRANCEHS_ROOT := $(LOG_DUMP).bench_branches
TOKEN_BRANCH := $(BRANCEHS_ROOT)/brnaches000/branch0000.txt
CMD_RUN := time
BUDGET = 50
EXEC_ARGS := # -n simple-star -s 20 --budget $(BUDGET)
ARGUMENTS_branches := --ghc-options "-DVERBOSE_SOLVING"
# ARGUMENTS_branches := --ghc-options "-DDUMP_GRAPH -DDUMP_STATES -DDUMP_CLUSTERS -DVERBOSE_SOLVING"
ARGUMENTS_profile := --executable-profiling
ARGUMENTS_scratch := # --profile
ARGUMENTS_scratch-prof := --profile
ARGUMENTS_fullspeed := --ghc-options "-DQUIET_MODE"
ARGUMENTS_normalspeed := --ghc-options "-rtsopts"
EXECUTABLE_scratch-prof := fluidb-scratch
EXECUTABLE_scratch := fluidb-scratch

DEFAULT_EXECUTABLE = fluidb-tpch-benchmark
# DEFAULT_EXECUTABLE = fluidb-star-benchmark

# bench_exec = $(word 1,$(shell find $(PWD)/.$(1)-stack-dir -type d -name bin | head -1)/$(if $(EXECUTABLE_$(1)),$(EXECUTABLE_$(1)),$(DEFAULT_EXECUTABLE)) first-build-$(1))
bench_exec = $(word 1,$(shell find $(PWD)/.stack-work -type d -name bin | head -1)/$(if $(EXECUTABLE_$(1)),$(EXECUTABLE_$(1)),$(DEFAULT_EXECUTABLE)) first-build-$(1))

define bench_goal
$(call bench_exec,$1): $(SOURCES) $(PWD)/fluidb.cabal
# 	stack build -j4 --work-dir .$(1)-stack-dir/ $(ARGUMENTS_$(1)) $(STACK_ARGS)
	stack build -j4 $(ARGUMENTS_$(1)) $(STACK_ARGS)
endef

$(eval $(call bench_goal,branches))
$(eval $(call bench_goal,fullspeed))
$(eval $(call bench_goal,normalspeed))
$(eval $(call bench_goal,scratch))
$(eval $(call bench_goal,scratch-prof))

.PHONY:
scratch-prof: | scratch-prof.profiterole.txt scratch-prof.profiterole.html
.PHONY:
scratch: $(call bench_exec,scratch)
	$(CMD_RUN) $(call bench_exec,scratch) $(EXEC_ARGS)
.PHONY:
profile: | profile.profiterole.txt profile.profiterole.html # profile.ps

.PHONY:
branches: $(TOKEN_BRANCH)

.PHONY:
deep-clean: clean
	rm -rf .branches-stack-dir .profile-stack-dir
.PHONY:
clean: clean-profile clean-branches

.PHONY:
clean-scratch:
	rm -f scratch.prof scratch.profiterole.html scratch.profiterole.txt

.PHONY:
clean-profile:
	rm -f profile.prof profile.profiterole.html profile.profiterole.txt

.PHONY:
clean-branches:
	rm -rf $(BRANCEHS_ROOT) $(LOG_DUMP) $(READDUMP_EXE)

.PHONY:
fullspeed: $(call bench_exec,fullspeed)
	$(CMD_RUN) $(call bench_exec,fullspeed) $(EXEC_ARGS)

.PHONY:
normalspeed: $(call bench_exec,normalspeed)
	$(CMD_RUN) $(call bench_exec,normalspeed) $(EXEC_ARGS)

# Profiterole on steroids can be found here
#   https://github.com/fakedrake/profiterole
%.profiterole.html %.profiterole.txt: %.prof
	profiterole $<

profile.prof: $(call bench_exec,profile)
	$(CMD_RUN) $(call bench_exec,profile) $(EXEC_ARGS) +RTS -t -hm -P || true
	mv $(shell basename $(call bench_exec,profile)).prof $@

scratch-prof.prof: $(call bench_exec,scratch-prof)
	$(CMD_RUN) $(call bench_exec,scratch-prof) $(EXEC_ARGS) +RTS -P -s || true
	mv $(shell basename $(call bench_exec,scratch-prof)).prof $@

profile.ps: profile.hp
	hp2pretty $<

$(READDUMP_EXE): tools/ReadDump/Main.hs Makefile
	stack ghc -- -rtsopts $< -o $@

.PRECIOUS:
$(LOG_DUMP): $(call bench_exec,branches)
	$(CMD_RUN) $(call bench_exec,branches) $(EXEC_ARGS) >& $@  || (tail $@)

$(TOKEN_BRANCH): $(READDUMP_EXE) $(LOG_DUMP)
	mkdir -p $(BRANCEHS_ROOT)
	time $< +RTS -M1G

repl:
	stack ghci --ghci-options=-ferror-spans --no-build --no-load --main-is=fluidb:exe:fluidb-benchmark "--ghci-options=+RTS -M1G -RTS"
