{ pkgs ? import <nixpkgs> {
    overlays = [
      (import (fetchTarball "https://github.com/oxalica/rust-overlay/archive/master.tar.gz"))
    ];
  }
}:
pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    rust-bin.stable.latest.default
    pkg-config
    openssl
  ];
}
