{ nixpkgs ? (import ./nixpkgs.nix {}) }:

let
  inherit (nixpkgs.haskell.lib) dontCheck doJailbreak
                                enableDWARFDebugging enableExecutableProfiling;
  inherit (nixpkgs.stdenv) lib;
  inherit (nixpkgs) fetchFromGitHub;

  cabalFilter = path: type:
    let pathBaseName = baseNameOf path;
    in !(lib.hasPrefix "dist-newstyle" pathBaseName) &&
       !(lib.hasPrefix ".git" pathBaseName) &&
       !(lib.hasPrefix ".ghc.environment" pathBaseName) &&
       !(lib.hasPrefix "dist" pathBaseName);

  localDir = builtins.filterSource cabalFilter;

  trec-eval = nixpkgs.enableDebugging (nixpkgs.callPackage ./trec-eval.nix {});

  haskellOverrides = self: super:
    let
      simplirPackages = {
        mkDerivation         = args: super.mkDerivation (args // {
          dontStrip = true;
          configureFlags =
            #["--profiling-detail=toplevel-functions"] ++
            (args.configureFlags or []) ++
            [ "--ghc-options=-g3" "--disable-executable-stripping" "--disable-library-stripping" ];
        });

        simplir              = let base = self.callCabal2nix "simplir" (localDir ./simplir) {};
                               in nixpkgs.haskell.lib.overrideCabal base (drv: { testDepends = [ trec-eval ]; });
        simplir-data-source  = self.callCabal2nix "simplir-data-source" (localDir ./simplir-data-source) {};
        simplir-html-clean   = self.callCabal2nix "simplir-html-clean" (localDir ./simplir-html-clean) {};
        simplir-trec         = self.callCabal2nix "simplir-trec" (localDir ./simplir-trec) {};
        simplir-galago       = self.callCabal2nix "simplir-galago" (localDir ./simplir-galago) {};
        simplir-tools        = enableExecutableProfiling (self.callCabal2nix "simplir-tools" (localDir ./simplir-tools) {});
        simplir-word-embedding = self.callCabal2nix "simplir-word-embedding" (localDir ./simplir-word-embedding) {};
        simplir-trec-streaming = self.callCabal2nix "simplir-trec-streaming" (localDir ./simplir-trec-streaming) {};
        simplir-kyoto-index  = self.callCabal2nix "simplir-kyoto-index" (localDir ./simplir-kyoto-index) {};
        simplir-leveldb-index = self.callCabal2nix "simplir-leveldb-index" (localDir ./simplir-leveldb-index) {};
        simplir-disk-index   = self.callCabal2nix "simplir-disk-index" (localDir ./simplir-disk-index) {};
        http-parsers         = self.callCabal2nix "http-parsers" ./vendor/http-parsers {};
        indexed-vector       = self.callCabal2nix "indexed-vector" ./vendor/indexed-vector {};
        fork-map             = self.callCabal2nix "fork-map" ./vendor/fork-map {};

        lzma = dontCheck super.lzma;
        ListLike = doJailbreak super.ListLike;
        text-icu   = dontCheck super.text-icu;
        pipes-zlib = doJailbreak super.pipes-zlib;
        pipes-text = doJailbreak super.pipes-text;
        pipes-lzma = doJailbreak super.pipes-lzma;
        pipes-interleave = doJailbreak super.pipes-interleave;
        b-tree = doJailbreak super.b-tree;
        warc = self.callCabal2nix "warc" (fetchFromGitHub {
          owner = "bgamari";
          repo = "warc";
          rev = "efee2bc4a71e054b65c20b90448ce05a34df09f4";
          sha256 = null;
        }) {};
      };
    in simplirPackages // { simplirPackages = simplirPackages; };

  haskellPackages = nixpkgs.haskell.packages.ghc843.override {overrides = haskellOverrides;};
in {
  inherit haskellPackages haskellOverrides;
  inherit trec-eval;
  inherit (haskellPackages) simplirPackages;
  env = haskellPackages.ghcWithHoogle (pkgs: builtins.attrValues haskellPackages.simplirPackages);
}
