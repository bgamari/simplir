let
  nixpkgs = import <nixpkgs> {};
  src = 
    nixpkgs.fetchFromGitHub {
      owner = "nixos";
      repo = "nixpkgs";
      rev = "9592e380c5ce1b0a437186feead82878fed77efc";
      sha256 = "13x8szfy3l15m0iwl59bnn52cwhbkb2fkd91nn85yd6brh2dg8jb";
    };
in
  import src