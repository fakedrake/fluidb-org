all:
	stack run --profile benchmark

ghcid:
	ghcid -c "stack ghci fluidb:lib" -T="graphMain"
