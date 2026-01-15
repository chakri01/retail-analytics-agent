"""
FAISS index with graceful fallback for dependency issues
"""
import numpy as np
from typing import List, Tuple, Dict, Any
import pickle
import os

class FaissIndex:
    """FAISS index for semantic search on metadata"""
    
    def __init__(self, model_name='all-MiniLM-L6-v2'):
        try:
            import faiss
            from sentence_transformers import SentenceTransformer
            self.faiss_available = True
            self.model = SentenceTransformer(model_name)
            self.index = None
        except ImportError as e:
            print(f"Warning: FAISS dependencies not available. Using simple text matching. Error: {e}")
            self.faiss_available = False
            self.model = None
            self.index = None
        
        self.chunks = []
        self.chunk_metadata = []
        self.index_path = "data/faiss_index"
        
    def build_index(self, chunks: List[str], metadata: List[Dict] = None):
        """Build FAISS index from text chunks"""
        print("Building FAISS index...")
        self.chunks = chunks
        self.chunk_metadata = metadata if metadata else [{} for _ in chunks]
        
        if not self.faiss_available:
            print("FAISS not available. Using simple text storage.")
            return
        
        try:
            import faiss
            # Generate embeddings
            embeddings = self.model.encode(chunks, convert_to_numpy=True)
            dimension = embeddings.shape[1]
            
            # Create FAISS index
            self.index = faiss.IndexFlatL2(dimension)
            self.index.add(embeddings.astype('float32'))
            
            # Save index
            self._save_index()
            print(f"FAISS index built with {len(chunks)} chunks")
        except Exception as e:
            print(f"Warning: Could not build FAISS index. Using simple matching. Error: {e}")
            self.faiss_available = False
    
    def search(self, query: str, k: int = 3) -> List[Tuple[str, float, Dict]]:
        """Search index for similar chunks"""
        if not self.faiss_available or self.index is None:
            # Simple text matching fallback
            results = []
            query_lower = query.lower()
            for idx, chunk in enumerate(self.chunks):
                if query_lower in chunk.lower():
                    results.append({
                        "text": chunk,
                        "score": 0.1,  # Low score for simple matching
                        "metadata": self.chunk_metadata[idx] if idx < len(self.chunk_metadata) else {}
                    })
                    if len(results) >= k:
                        break
            return results
        
        try:
            import faiss
            query_embedding = self.model.encode([query], convert_to_numpy=True).astype('float32')
            distances, indices = self.index.search(query_embedding, k)
            
            results = []
            for idx, distance in zip(indices[0], distances[0]):
                if idx < len(self.chunks):
                    results.append({
                        "text": self.chunks[idx],
                        "score": float(distance),
                        "metadata": self.chunk_metadata[idx]
                    })
            
            return results
        except Exception as e:
            print(f"Error in FAISS search: {e}")
            return []
    
    def get_relevant_context(self, query: str, k: int = 5) -> str:
        """Get relevant context for a query"""
        results = self.search(query, k)
        if not results:
            return ""
        
        context_parts = []
        for i, result in enumerate(results):
            context_parts.append(f"{i+1}. {result['text']}")
        
        return "\n".join(context_parts) if context_parts else ""
    
    def _save_index(self):
        """Save FAISS index and chunks to disk"""
        if not self.faiss_available:
            return
        
        os.makedirs(os.path.dirname(self.index_path), exist_ok=True)
        
        try:
            import faiss
            # Save FAISS index
            faiss.write_index(self.index, f"{self.index_path}.faiss")
            
            # Save chunks and metadata
            with open(f"{self.index_path}.pkl", 'wb') as f:
                pickle.dump({
                    'chunks': self.chunks,
                    'metadata': self.chunk_metadata
                }, f)
        except Exception as e:
            print(f"Could not save FAISS index: {e}")
    
    def _load_index(self):
        """Load FAISS index and chunks from disk"""
        if not os.path.exists(f"{self.index_path}.faiss"):
            return
        
        try:
            import faiss
            # Load FAISS index
            self.index = faiss.read_index(f"{self.index_path}.faiss")
            
            # Load chunks and metadata
            with open(f"{self.index_path}.pkl", 'rb') as f:
                data = pickle.load(f)
                self.chunks = data['chunks']
                self.chunk_metadata = data['metadata']
            
            print(f"Loaded FAISS index with {len(self.chunks)} chunks")
        except Exception as e:
            print(f"Could not load FAISS index: {e}")
            self.index = None
    
    def exists(self) -> bool:
        """Check if index exists on disk"""
        return os.path.exists(f"{self.index_path}.faiss") and self.faiss_available