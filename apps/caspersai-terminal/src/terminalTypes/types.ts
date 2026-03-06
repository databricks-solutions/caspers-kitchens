export type TerminalTypeAuthPolicy = "both" | "user" | "m2m";

export type TerminalType = {
  id: string;
  name: string;
  description?: string;
  badge?: string;
  icon?: string;
  authPolicy: TerminalTypeAuthPolicy;
  default: boolean;
  order?: number;
  builtIn: boolean;
};

export type ResolvedTerminalType = TerminalType & {
  entrypointPath?: string;
};

export interface TerminalTypeRegistry {
  listTypes(): TerminalType[];
  resolveType(id: string): ResolvedTerminalType | undefined;
  getDefaultType(): ResolvedTerminalType;
}
