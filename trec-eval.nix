{ stdenv, fetchFromGitHub }:

stdenv.mkDerivation {
  name = "trec-eval";
  src = fetchFromGitHub {
    owner = "bgamari";
    repo = "trec_eval";
    rev = "master";
    sha256 = null;
  };
  /* Pending #16
  src = fetchFromGitHub {
    owner = "usnistgov";
    repo = "trec_eval";
    rev = "v9.0.5";
    sha256 = null;
  };
  */
  installPhase = ''
    mkdir -p $out/bin
    cp trec_eval $out/bin
  '';
}
