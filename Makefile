HASKELL_FILES=$(shell find . -name "*.hs")

all: benchmark.profiterole.txt

benchmark.prof: $(HASKELL_FILES)
	stack run --profile benchmark --rts-options -p

# Profiterole on steroids can be found here
#   https://github.com/fakedrake/profiterole
%.profiterole.html %.profiterole.txt: %.prof
	profiterole $<


ghcid:
	ghcid -c "stack ghci fluidb:lib" -T="graphMain"
