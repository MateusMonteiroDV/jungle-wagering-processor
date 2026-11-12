import {
  CanActivate,
  ExecutionContext,
  Inject,
  Injectable,
} from "@nestjs/common";
import type { Request } from "express";
import type {
  ProviderIdentity,
  ProviderIdentityPort,
} from "../../application/ports";
import { TOKENS } from "../../application/ports";

export class NoOpProviderIdentityAdapter implements ProviderIdentityPort {
  async verifyRequest(
    _headers: Record<string, string | string[] | undefined>,
  ): Promise<ProviderIdentity> {
    return {
      authenticated: true,
    };
  }
}

@Injectable()
export class AuthGuard implements CanActivate {
  constructor(
    @Inject(TOKENS.ProviderIdentityPort)
    private readonly identityPort: ProviderIdentityPort,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest<Request>();
    const path = req.path || req.url || "";
    if (path.startsWith("/health") || path.startsWith("/metrics")) {
      return true;
    }
    const identity = await this.identityPort.verifyRequest(req.headers);
    (req as Request & { providerIdentity?: ProviderIdentity }).providerIdentity =
      identity;
    return identity.authenticated;
  }
}
