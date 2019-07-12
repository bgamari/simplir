{ nixpkgs ? (import ./nixpkgs.nix {}) }:

let
  inherit (nixpkgs.haskell.lib) dontCheck doJailbreak
                                enableDWARFDebugging enableExecutableProfiling;
  inherit (nixpkgs.stdenv) lib;
  inherit (nixpkgs) fetchFromGitHub;

  localDir = nixpkgs.nix-gitignore.gitignoreSourcePure [ ./.gitignore ];

  trec-eval = nixpkgs.enableDebugging (nixpkgs.callPackage ./trec-eval.nix {});

  haskellOverrides = self: super:
    let
      otherOverrides = {
        mkDerivation         = args: super.mkDerivation (args // {
          dontStrip = true;
          configureFlags =
            #["--profiling-detail=toplevel-functions"] ++
            (args.configureFlags or []) ++
            [ "--ghc-options=-g3"
              "--disable-executable-stripping"
              "--disable-library-stripping"
              #"--ghc-options=-eventlog"
            ];
        });
      };

      simplirPackages = {
        simplir              = let base = self.callCabal2nix "simplir" (localDir ./simplir) {};
                               in nixpkgs.haskell.lib.overrideCabal base (drv: { testDepends = [ trec-eval ]; });
        simplir-data-source  = self.callCabal2nix "simplir-data-source" (localDir ./simplir-data-source) {};
        simplir-html-clean   = self.callCabal2nix "simplir-html-clean" (localDir ./simplir-html-clean) {};
        simplir-trec         = self.callCabal2nix "simplir-trec" (localDir ./simplir-trec) {};
        simplir-galago       = self.callCabal2nix "simplir-galago" (localDir ./simplir-galago) {};
        simplir-tools        = self.callCabal2nix "simplir-tools" (localDir ./simplir-tools) {};
        simplir-word-embedding = self.callCabal2nix "simplir-word-embedding" (localDir ./simplir-word-embedding) {};
        simplir-trec-streaming = self.callCabal2nix "simplir-trec-streaming" (localDir ./simplir-trec-streaming) {};
        simplir-kyoto-index  = self.callCabal2nix "simplir-kyoto-index" (localDir ./simplir-kyoto-index) {};
        simplir-leveldb-index = self.callCabal2nix "simplir-leveldb-index" (localDir ./simplir-leveldb-index) {};
        simplir-disk-index   = self.callCabal2nix "simplir-disk-index" (localDir ./simplir-disk-index) {};
        simplir-eval         = self.callCabal2nix "simplir-eval" (localDir ./simplir-eval) {};
        simplir-learning-to-rank
                             = self.callCabal2nix "simplir-learning-to-rank" (localDir ./simplir-learning-to-rank) {};
        http-parsers         = self.callCabal2nix "http-parsers" ./vendor/http-parsers {};
        indexed-vector       = self.callCabal2nix "indexed-vector" ./vendor/indexed-vector {};
        fork-map             = self.callCabal2nix "fork-map" ./vendor/fork-map {};

        lzma = dontCheck super.lzma;
        ListLike = doJailbreak super.ListLike;
        text-icu   = dontCheck super.text-icu;
        pipes-zlib = doJailbreak super.pipes-zlib;
        pipes-text = doJailbreak (super.callHackage "pipes-text" "0.0.2.5" {});
        pipes-lzma = doJailbreak super.pipes-lzma;
        pipes-interleave = doJailbreak super.pipes-interleave;
        b-tree = doJailbreak super.b-tree;
        warc = self.callCabal2nix "warc" (fetchFromGitHub {
          owner = "bgamari";
          repo = "warc";
          rev = "725d9d1265fda5fe3cb8cc11eff7d9bf2f714356";
          sha256 = "006k5brxxr023i62pq8q4v6sn1svgyg1lyv4b1nll5n5l3bj9jvw";
        }) {};
        monoidal-containers = self.callCabal2nix "warc" (fetchFromGitHub {
          owner = "bgamari";
          repo = "monoidal-containers";
          rev = "a34c9fbe191725ef9a9c7783e103c24796bd91e3";
          sha256 = "1ar2w4rx0mh4nvwzpc125l3hj9xslargl43vnssmh9l6ynhi8ksv";
        }) {};

        pinch = doJailbreak (self.callHackage "pinch" "0.3.4.0" {});
        pipes-safe = self.callHackage "pipes-safe" "2.3.1" {};
      };
    in otherOverrides // simplirPackages // { simplirPackages = simplirPackages; };

  ghcVersion = "ghc864";
  haskellPackages = nixpkgs.haskell.packages."${ghcVersion}".override {overrides = haskellOverrides;};
in {
  inherit ghcVersion haskellPackages haskellOverrides;
  inherit trec-eval;
  inherit (haskellPackages) simplirPackages;
  env = haskellPackages.ghcWithHoogle (pkgs: builtins.attrValues haskellPackages.simplirPackages);
}
