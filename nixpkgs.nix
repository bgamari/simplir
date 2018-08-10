let
  nixpkgs = import <nixpkgs> {};
  src = 
    nixpkgs.fetchFromGitHub {
      owner = "nixos";
      repo = "nixpkgs";
      rev = "2428f5dda13475afba2dee93f4beb2bd97086930";
      sha256 = "1iwl5yaz36lf7v4hps3z9dl3zyq363jmr5m7y4anf0lpn4lczh18";
    };
in
  import src