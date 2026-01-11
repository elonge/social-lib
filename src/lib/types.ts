export interface Book {
  id: string;
  title: string;
  author: string;
  isbn?: string;
  coverImage?: string;
  language?: string;
  confidence?: string;
  isUnidentified?: boolean;
}
