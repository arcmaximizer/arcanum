// db.ts

export interface DatabaseService {
  addEvent(): Promise<number>;
}

export class NaiveDatabaseService {
  async addEvent() {
  }
}
