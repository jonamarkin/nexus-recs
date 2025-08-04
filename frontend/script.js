document.addEventListener('DOMContentLoaded', () => {
    const userIdInput = document.getElementById('userId');
    const categoryBtn = document.getElementById('categoryBtn');
    const collaborativeBtn = document.getElementById('collaborativeBtn');
    const recommendationsList = document.getElementById('recommendationsList');
    
    const API_BASE_URL = 'http://127.0.0.1:8000';

    async function fetchRecommendations(endpoint) {
        const userId = userIdInput.value;
        if (!userId) {
            alert('Please enter a User ID.');
            return;
        }

        recommendationsList.innerHTML = '<p>Loading recommendations...</p>';
        
        try {
            const response = await fetch(`${API_BASE_URL}/recommendations/${endpoint}/${userId}`);
            
            if (!response.ok) {
                throw new Error(`API returned status ${response.status}`);
            }

            const data = await response.json();
            
            if (data.message) {
                recommendationsList.innerHTML = `<p>${data.message}</p>`;
                return;
            }

            // This is the crucial fix: access the recommendations array from the data object
            displayRecommendations(data.recommendations);

        } catch (error) {
            console.error('Error fetching recommendations:', error);
            recommendationsList.innerHTML = `<p style="color: red;">Error: ${error.message}</p>`;
        }
    }

    function displayRecommendations(recommendations) {
        if (!Array.isArray(recommendations) || recommendations.length === 0) {
            recommendationsList.innerHTML = '<p>No recommendations found for this user.</p>';
            return;
        }
        
        recommendationsList.innerHTML = ''; // Clear previous results
        
        recommendations.forEach(item => {
            const card = document.createElement('div');
            card.className = 'recommendation-card';
            card.innerHTML = `
                <h3>${item.title}</h3>
                <p><strong>ID:</strong> ${item.item_id}</p>
                <p><strong>Category:</strong> ${item.category}</p>
                <p><strong>Description:</strong> ${item.description || 'N/A'}</p>
            `;
            recommendationsList.appendChild(card);
        });
    }

    categoryBtn.addEventListener('click', () => {
        fetchRecommendations('category');
    });

    collaborativeBtn.addEventListener('click', () => {
        fetchRecommendations('collaborative');
    });
});