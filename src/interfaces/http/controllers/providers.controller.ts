import { Controller, Get, Inject, Param } from "@nestjs/common";
import { GetTransactionQuery } from "../../../application/use-cases/queries/get-transaction";
import { updateLogContext } from "../../../infrastructure/observability/context";

@Controller("providers")
export class ProvidersController {
  constructor(
    @Inject(GetTransactionQuery)
    private readonly getTransaction: GetTransactionQuery,
  ) {}

  @Get(":providerId/wagering/transactions/:externalTransactionId")
  async findByExternalId(
    @Param("providerId") providerId: string,
    @Param("externalTransactionId") externalTransactionId: string,
  ) {
    updateLogContext({ providerId });
    return this.getTransaction.executeByProviderAndExternalId(
      providerId,
      externalTransactionId,
    );
  }
}
