{ nixpkgs ? (import <nixpkgs> {}) }:

let
  inherit (nixpkgs.haskell.lib) dontCheck doJailbreak;
  inherit (nixpkgs.stdenv) lib;

  cabalFilter = path: type:
    let pathBaseName = baseNameOf path;
    in !(lib.hasPrefix "dist-newstyle" pathBaseName) &&
       !(lib.hasPrefix ".git" pathBaseName) &&
       !(lib.hasPrefix "dist" pathBaseName);

  localDir = builtins.filterSource cabalFilter;

  cborgSrc = nixpkgs.fetchgit { url = https://github.com/well-typed/cborg.git; rev = "6c05de8e9490d32e80611e5b41773103ebe72ec3"; sha256 = "18cwgvk7gahab742hwj5257x6mpcf4yci076lsbkaf1fb70kwmjp"; };

  haskellOverrides = self: super:
    let
      localPkgs = {
        simplir              = self.callCabal2nix "simplir" (localDir ./.) {};
        simplir-data-source  = self.callCabal2nix "simplir-data-source" (localDir ./simplir-data-source) {};
        simplir-html-clean   = self.callCabal2nix "simplir-html-clean" (localDir ./simplir-html-clean) {};
        simplir-trec         = self.callCabal2nix "simplir-trec" (localDir ./simplir-trec) {};
        simplir-galago       = self.callCabal2nix "simplir-galago" (localDir ./simplir-galago) {};
        simplir-tools        = self.callCabal2nix "simplir-tools" (localDir ./simplir-tools) {};
        simplir-word-embedding = self.callCabal2nix "simplir-word-embedding" (localDir ./simplir-word-embedding) {};
        simplir-trec-streaming = self.callCabal2nix "simplir-trec-streaming" (localDir ./simplir-trec-streaming) {};
        http-parsers         = self.callCabal2nix "http-parsers" ./vendor/http-parsers {};
        indexed-vector       = self.callCabal2nix "indexed-vector" ./vendor/indexed-vector {};
        fork-map             = self.callCabal2nix "fork-map" ./vendor/fork-map {};

        lzma = dontCheck super.lzma;
        text-icu   = dontCheck super.text-icu;
        pipes-zlib = doJailbreak super.pipes-zlib;
        optparse-applicative = self.callHackage "optparse-applicative" "0.14.0.0" {};
        cborg = self.callCabal2nix "cborg" (cborgSrc + /cborg) {};
        serialise = self.callCabal2nix "serialise" (cborgSrc + /serialise) {};
      };
    in localPkgs // { localPkgs = localPkgs; };

  haskellPackages = nixpkgs.haskell.packages.ghc821.override {overrides = haskellOverrides;};
in {
  inherit haskellPackages haskellOverrides;
  inherit (haskellPackages) localPkgs;
  env = haskellPackages.ghcWithHoogle (pkgs: builtins.attrValues haskellPackages.localPkgs);
}
