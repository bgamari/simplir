let
  rev = "d8a7a01fecb4dd05dead58f70ea4bfd0b8336459";
  sha256 = "0i1zfpi3na8fvs0xhain7nwa0d744jzkj24qrjqbw7r00p1vs26x";
  tarball = builtins.fetchTarball {
    url = "https://github.com/nixos/nixpkgs-channels/archive/${rev}.tar.gz";
    inherit sha256;
  };
in import tarball
