{ nixpkgs ? import <nixpkgs> {} }:
with nixpkgs; let
  haskellPackages = haskell.packages.ghc8104;
  # Haskell tools
  ghc = haskellPackages.ghcWithPackages (ps: with ps; [
    base
    shake
    hasktags
    profiterole
    floskell
    haskell-language-server
    # implicit-hie
  ]);

  nixPackages = [
    ghc
    stack
  ];
in
haskell.lib.buildStackProject {
  name = "fluidb";
  ghc = ghc;
  stack = stack;
  buildInputs = nixPackages;
}
