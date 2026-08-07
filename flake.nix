{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" "clippy" ];
        };
      in {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            rustToolchain
            pkg-config
            openssl
            sqlx-cli
            cargo-watch
            postgresql
            nodejs_22
            bun
            icecast

            # Playwright / Chromium system deps
            nss nspr atk at-spi2-atk cups libdrm pango expat
            libxkbcommon libxcb libx11 libxcomposite
            libxdamage libxext libxfixes libxrandr
            alsa-lib libgbm dbus systemd libGL glib gtk3 cairo
            libthai freetype fontconfig harfbuzz bzip2 zlib libpng libjpeg
          ];
          LD_LIBRARY_PATH = with pkgs; lib.makeLibraryPath [
            nss nspr atk at-spi2-atk cups libdrm pango expat
            libxkbcommon libxcb libx11 libxcomposite
            libxdamage libxext libxfixes libxrandr
            alsa-lib libgbm dbus systemd libGL stdenv.cc.cc
            glib gtk3 cairo libthai freetype fontconfig harfbuzz
            bzip2 zlib libpng libjpeg
          ];


          shellHook = ''
            set -a
            [ -f .env ] && source .env
            set +a
            export SURCAST_PGDATA="$PWD/.pgdata"
            export PGHOST="$SURCAST_PGDATA"
            export PGPORT="5433"
            export PATH="$PWD/scripts:$PATH"

            echo "  dev                     – start everything on :6767"
            echo "  pg-start|stop|status       – manage PostgreSQL"
          '';
        };
      });
}
