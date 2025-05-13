// Assuming the Vite proxy is configured to forward /api requests
const API_BASE_URL = '/api'; 

/**
 * Fetches the list of available indices from the Vortex server.
 * @returns {Promise<string[]>} A promise that resolves to an array of index names.
 * @throws {Error} If the network response is not ok.
 */
export const fetchIndices = async () => {
    const response = await fetch(`${API_BASE_URL}/indices`);
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to fetch indices: ${response.status} ${errorText}`);
    }
    // Assuming the API returns an array of strings like ["index1", "index2"]
    return response.json(); 
};

/**
 * Fetches vector data for a given index.
 * NOTE: This is a placeholder. The backend API currently lacks an efficient 
 * endpoint for fetching multiple vectors (e.g., for sampling or pagination).
 * This function assumes such an endpoint exists or will be added at 
 * `/api/indices/{indexName}/vectors` which might accept query params like `limit`.
 * 
 * @param {string} indexName - The name of the index.
 * @param {number} [limit=1000] - Optional limit for the number of vectors to fetch.
 * @returns {Promise<Array<{id: string, vector: number[], metadata?: object}>>} A promise resolving to vector data, including optional metadata.
 * @throws {Error} If the network response is not ok.
 */
export const fetchVectorsSample = async (indexName, limit = 1000) => {
    // TODO: Update URL and handling based on the actual backend implementation
    //       for bulk vector fetching.
    const response = await fetch(`${API_BASE_URL}/indices/${indexName}/vectors?limit=${limit}`); 
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to fetch vectors for ${indexName}: ${response.status} ${errorText}`);
    }
    // Assuming API returns an array of objects like [{ id: "vec1", vector: [0.1, 0.2, ...] }, ...]
    return response.json(); 
};

/**
 * Creates a new vector index.
 * @param {string} indexName - The desired name for the new index.
 * @param {number} dimensions - The dimensionality of vectors for this index.
 * @param {string} metric - The distance metric ('euclidean', 'cosine', 'dot').
 * @param {object} [config] - Optional HNSW configuration parameters.
 * @returns {Promise<object>} A promise resolving to the success response from the server.
 * @throws {Error} If the network response is not ok or request fails.
 */
export const createIndex = async (indexName, dimensions, metric, config = {}) => {
    const response = await fetch(`${API_BASE_URL}/indices`, {
        method: 'POST',
        headers: { 
            'Content-Type': 'application/json',
            'Accept': 'application/json', 
        },
        // Backend expects: { name: string, dimensions: number, metric: string, config: object }
        body: JSON.stringify({ 
            name: indexName, 
            dimensions: dimensions, 
            metric: metric, 
            config: config // Send empty object if not provided
        })
    });

    if (!response.ok) {
        // Attempt to parse error response from backend if available
        let errorData;
        try {
            errorData = await response.json();
        } catch (e) {
            // Ignore if response is not JSON
        }
        const errorText = errorData?.message || response.statusText || 'Failed to create index';
        throw new Error(`Index creation failed: ${response.status} ${errorText}`);
    }
    
    // Assuming API returns a success message like { message: "Index '...' created" }
    return response.json(); 
};


/**
 * Fetches statistics for a specific index.
 * @param {string} indexName - The name of the index.
 * @returns {Promise<object>} A promise resolving to the index statistics object.
 * @throws {Error} If the network response is not ok.
 */
export const fetchIndexStats = async (indexName) => {
    const response = await fetch(`${API_BASE_URL}/indices/${indexName}/stats`);
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to fetch stats for ${indexName}: ${response.status} ${errorText}`);
    }
    // Assuming API returns an object like { vector_count: 1000, dimensions: 128, ... }
    return response.json();
};

/**
 * Adds or updates a vector in the specified index.
 * @param {string} indexName - The name of the index.
 * @param {string} vectorId - The ID of the vector.
 * @param {number[]} vectorData - The vector data array.
 * @param {object} [metadata] - Optional JSON metadata to associate with the vector.
 * @returns {Promise<object>} A promise resolving to the success response from the server.
 * @throws {Error} If the network response is not ok or request fails.
 */
export const addVector = async (indexName, vectorId, vectorData, metadata) => {
    const payload = { 
        id: vectorId, 
        vector: vectorData 
    };
    if (metadata !== undefined) {
        payload.metadata = metadata;
    }

    const response = await fetch(`${API_BASE_URL}/indices/${indexName}/vectors`, { // Uses PUT
        method: 'PUT',
        headers: { 
            'Content-Type': 'application/json',
            'Accept': 'application/json', 
        },
        // Backend expects: { id: string, vector: number[], metadata?: object }
        body: JSON.stringify(payload)
    });

    if (!response.ok) {
        let errorData;
        try {
            errorData = await response.json();
        } catch (e) { /* Ignore */ }
        const errorText = errorData?.message || response.statusText || 'Failed to add/update vector';
        throw new Error(`Add/Update vector failed: ${response.status} ${errorText}`);
    }
    
    return response.json(); // Contains success message
};


/**
 * Performs a k-NN search within a specific index.
 * @param {string} indexName - The name of the index to search within.
 * @param {number[]} queryVector - The vector to search for.
 * @param {number} [k=10] - The number of nearest neighbors to return.
 * @param {object} [filter] - Optional metadata filter object.
 * @returns {Promise<Array<{id: string, score: number, metadata?: object}>>} A promise resolving to search results, including optional metadata.
 * @throws {Error} If the network response is not ok.
 */
export const searchVectors = async (indexName, queryVector, k = 10, filter) => {
    const payload = { 
        query_vector: queryVector, 
        k: k 
    };
    if (filter !== undefined) {
        payload.filter = filter;
    }

    const response = await fetch(`${API_BASE_URL}/indices/${indexName}/search`, {
        method: 'POST',
        headers: { 
            'Content-Type': 'application/json',
            'Accept': 'application/json', // Explicitly accept JSON
        },
        body: JSON.stringify(payload)
    });
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Search failed for ${indexName}: ${response.status} ${errorText}`);
    }
    // Assuming API returns an array like [{ id: "vec5", score: 0.98 }, { id: "vec12", score: 0.95 }, ...]
    return response.json(); 
};

/**
 * Adds multiple vectors to the specified index in a batch.
 * @param {string} indexName - The name of the index.
 * @param {Array<object>} vectors - An array of vector items, where each item is { id: string, vector: number[], metadata?: object }.
 * @returns {Promise<object>} A promise resolving to the batch operation response from the server (e.g., { success_count, failure_count, message }).
 * @throws {Error} If the network response is not ok or request fails.
 */
export const batchAddVectors = async (indexName, vectors) => {
    const response = await fetch(`${API_BASE_URL}/indices/${indexName}/vectors/batch`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        },
        body: JSON.stringify({ vectors: vectors }) // Backend expects { vectors: [...] }
    });

    if (!response.ok) {
        let errorData;
        try {
            errorData = await response.json();
        } catch (e) { /* Ignore */ }
        const errorText = errorData?.message || response.statusText || 'Failed to batch add vectors';
        throw new Error(`Batch add vectors failed for ${indexName}: ${response.status} ${errorText}`);
    }

    return response.json(); // Contains BatchOperationResponse
};
