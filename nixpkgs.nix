let
  rev = "be346a1f4bd9bf272c1388b7791cdb0f28bfa2fb";
  sha256 = "1f0p8x4h5k190vizlan5yljqvsay2krn93jl3m4yqlg808yglsr3";
  tarball = builtins.fetchTarball {
    url = "https://github.com/nixos/nixpkgs-channels/archive/${rev}.tar.gz";
    inherit sha256;
  };
in import tarball
