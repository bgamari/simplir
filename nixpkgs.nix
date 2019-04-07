let
  rev = "d956f2279b8ac02bd9e48cf2a09dcb66383ab6be";
  sha256 = "1pqrgimx9j8l8vrdwx4lrwl5lj7d5cpwx86azx8z5wrciv98za8w";
  tarball = builtins.fetchTarball {
    url = "https://github.com/nixos/nixpkgs-channels/archive/${rev}.tar.gz";
    inherit sha256;
  };
in import tarball
