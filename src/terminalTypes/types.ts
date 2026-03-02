export type TerminalType = {
  id: string;
  name: string;
  description?: string;
  badge?: string;
  icon?: string;
  default: boolean;
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
