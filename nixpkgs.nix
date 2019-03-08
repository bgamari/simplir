let
  rev = "34aa254f9ebf5899636a9927ceefbc9df80230f4";
  sha256 = "1ajl37n7bycww9c0igigprm02g3s2bv5295v2m1p3hs74li0pyhr";
  tarball = builtins.fetchTarball {
    url = "https://github.com/nixos/nixpkgs-channels/archive/${rev}.tar.gz";
    inherit sha256;
  };
in import tarball
