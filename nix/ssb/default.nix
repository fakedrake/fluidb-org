{stdenv, lib, fetchFromGitHub, gcc, makeWrapper}:
stdenv.mkDerivation {
  name = "ssb-dbgen";
  src = fetchFromGitHub {
    owner = "fakedrake";
    repo = "ssb-dbgen";
    rev = "HEAD";
    sha256 = "0yr5dr7gjf8a9fnvrz8fdvc6ja7vz0bs2iszf7m9fmzr4sp1fpiv"; # lib.fakeSha256;
  };
  buildInputs = [gcc makeWrapper];
  installPhase = ''
  mkdir -p $out/bin
  mkdir -p $out/var
  cp dbgen $out/bin/dbgen
  cp dists.dss $out/var/dists.dss
  '';
  postFixup = ''
  wrapProgram $out/bin/dbgen --set DSS_CONFIG "$out/var/"
  '';
}
