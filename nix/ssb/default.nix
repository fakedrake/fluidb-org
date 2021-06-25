{stdenv, lib, fetchFromGitHub, gcc, makeWrapper}:
stdenv.mkDerivation {
  name = "ssb-dbgen";
  src = fetchFromGitHub {
    owner = "fakedrake";
    repo = "ssb-dbgen";
    rev = "HEAD";
    sha256 = "10x9f2mqb4vxz1a1grvsnnyrf3pcdd0pc39v0alyd940hx65qzi7"; # lib.fakeSha256;
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
