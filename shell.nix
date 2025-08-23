{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    python312
    python312Packages.pip
    python312Packages.hatchling
    python312Packages.build
    python312Packages.twine
  ];

  shellHook = ''
    echo "mergething development environment"
    echo "Run 'pip install -e .' to install in development mode"
  '';
}