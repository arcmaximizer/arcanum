export interface ArcanumDB {
  nodes: {
    id: string;
  };
  edges: {
    parent_id: string;
    child_id: string;
  };
  heads: {
    id: string;
  };
  state_diffs: {
    checkpoint: string;
    app: string;
    key: string;
    value: string;
  };
}

export interface StateView {
  id: string;
}