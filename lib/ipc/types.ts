export type IPCBody = unknown;

export interface IPCRequest {
  id: string;
  body: IPCBody;
}

export interface IPCResponse {
  id: string;
  body: IPCBody;
}

export type IPCMethodHandler = (
  body: IPCBody
) => IPCBody | Promise<IPCBody>;

export interface IPCOptions {
  timeout?: number;
}

export interface IIpc {
  call<R = IPCBody>(method: string, body?: IPCBody): Promise<R>;
  on(method: string, handler: IPCMethodHandler): void;
  off(method: string): void;
  terminate(): void;
}
