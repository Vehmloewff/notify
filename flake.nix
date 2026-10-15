{
  description = "Route text notifications into shell commands";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        notify = pkgs.buildGoModule {
          pname = "notify";
          version = self.shortRev or self.dirtyShortRev or "dev";
          src = self;
          vendorHash = null;
          env.CGO_ENABLED = 0;
          ldflags = [ "-s" "-w" ];

          meta = {
            license = pkgs.lib.licenses.mit;
            description = "Route text notifications into shell commands";
            mainProgram = "notify";
          };
        };
      in
      {
        packages = {
          inherit notify;
          default = notify;
        };

        apps.default = flake-utils.lib.mkApp { drv = notify; };

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [ go gopls gotools ];
        };
      });
}
