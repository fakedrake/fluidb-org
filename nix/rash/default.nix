
{ nixpkgs ? import <nixpkgs> {}
, racket2nix ? nixpkgs.fetchFromGitHub {
  owner = "fractalide"; repo = "racket2nix";
  rev = "59c614406d4796f40620f6490b0b05ecb51ed976";
  sha256 = "0z5y1jm60vkwvi66q39p88ygkgyal81486h577gikmpqjxkg9d6i"; }
}:

let
  r2n = import racket2nix {};
in r2n.buildRacketPackage "spvector"
