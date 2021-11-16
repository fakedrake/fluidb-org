{ nixpkgs ? import <nixpkgs> {} }:
with nixpkgs; let
  haskellPackages = haskell.packages.ghc8107;
  # Haskell tools
  ghc = haskellPackages.ghcWithPackages (ps: with ps; [
    template-haskell
    base
    # czipwith
    # shake
    hasktags
    profiteur
    profiterole
    floskell
    haskell-language-server
    # implicit-hie
  ]);
  rash = callPackage ./nix/rash {};
  ssb = callPackage ./nix/ssb {};
  nixPackages = [
    # rash
    clang
    ssb
    ghc
    stack
    ccls
    graphviz
  ];
in
haskell.lib.buildStackProject {
  CPATH = "${builtins.toString ./.}/bama/include";
  name = "fluidb";
  ghc = ghc;
  stack = stack;
  buildInputs = nixPackages;
}
