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
    implicit-hie
  ]);
  rnix = import (fetchTarball {
    url = https://github.com/nix-community/rnix-lsp/archive/master.tar.gz;
    sha256="08dahxyn75w3621jc62w0vp0nn2zim2vn7kfw2s2rk93bdjf7bq7";
  });
  ssb = callPackage ./nix/ssb {};
  nixPackages = [
    clang
    ssb
    ghc
    stack
    ccls
  ];
in
haskell.lib.buildStackProject {
  CPATH = "${builtins.toString ./.}/bama/include";
  name = "fluidb";
  ghc = ghc;
  stack = stack;
  buildInputs = nixPackages;
}
