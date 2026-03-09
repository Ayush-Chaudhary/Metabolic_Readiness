# ============================================================================
# SIMON Health Habits - Insight Generator
# ============================================================================
# This module handles the LLM-based message generation using Databricks
# Foundation Model APIs (Meta Llama 3.3 70B).
# ============================================================================

from typing import Dict, Any, Optional
import yaml
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InsightGenerator:
    """
    Generates personalized health messages using Databricks Foundation Model APIs.
    
    Uses a "Logic-First, LLM-Second" approach where:
    1. LogicEngine selects the facts to include
    2. This class formats the prompt and calls the LLM
    3. Length validation ensures compliance with character limits
    """
    
    def __init__(self, config_path: str = "prompts.yml"):
        """
        Initialize the insight generator.
        
        Args:
            config_path: Path to the prompts YAML configuration file
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.model_config = self.config['model']
        self.constraints = self.config['message_constraints']
        self.system_prompt_template = self.config['system_prompt']
        self.user_prompt_template = self.config['user_prompt_template']
        
        # Initialize the Databricks client (lazy initialization)
        self._client = None
        self._endpoint = None
    
    def _get_client(self):
        """Get or create the Databricks serving client."""
        if self._client is None:
            try:
                from databricks.sdk import WorkspaceClient
                from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
                
                self._client = WorkspaceClient()
                self._endpoint = self.model_config['endpoint_name']
                logger.info(f"Initialized Databricks client with endpoint: {self._endpoint}")
            except ImportError:
                logger.warning("Databricks SDK not available. Using mock mode.")
                self._client = "mock"
        
        return self._client
    
    def _format_system_prompt(self) -> str:
        """Format the system prompt."""
        return self.system_prompt_template
    
    def _format_user_prompt(
        self,
        daily_rating: str,
        rating_description: str,
        positive_actions: list,
        opportunity: dict,
        greeting: str
    ) -> str:
        """
        Format the user prompt with selected content.
        
        Args:
            daily_rating: The calculated daily rating (e.g., "Committed")
            rating_description: Description of the rating
            positive_actions: List of selected positive action dicts
            opportunity: Selected opportunity dict
            greeting: Time-appropriate greeting
            
        Returns:
            Formatted prompt string
        """
        # Format positive facts as bullet list
        if positive_actions:
            facts_list = "\n".join([f"- {action['text']}" for action in positive_actions])
        else:
            facts_list = "- User logged into the app today"
        
        # Get opportunity text
        opp_text = opportunity.get('text', 'Browse the Explore section to learn more.')
        
        return self.user_prompt_template.format(
            daily_rating=daily_rating,
            rating_description=rating_description,
            positive_facts_list=facts_list,
            opportunity_suggestion=opp_text,
            greeting=greeting
        )
    
    def _call_llm(self, system_prompt: str, user_prompt: str) -> str:
        """
        Call the Databricks Foundation Model API.
        
        Args:
            system_prompt: The system instructions
            user_prompt: The user message with context
            
        Returns:
            Generated message string
        """
        client = self._get_client()
        
        if client == "mock":
            # Return mock response for testing
            return self._generate_mock_response(user_prompt)
        
        try:
            from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
            
            response = client.serving_endpoints.query(
                name=self._endpoint,
                messages=[
                    ChatMessage(
                        role=ChatMessageRole.SYSTEM,
                        content=system_prompt
                    ),
                    ChatMessage(
                        role=ChatMessageRole.USER,
                        content=user_prompt
                    )
                ],
                max_tokens=self.model_config['max_tokens'],
                temperature=self.model_config['temperature'],
                # top_p=self.model_config.get('top_p', 0.9)
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Error calling LLM: {e}")
            raise
    
    def _call_llm_with_openai_format(self, system_prompt: str, user_prompt: str) -> str:
        """
        Alternative method using OpenAI-compatible API format.
        Use this if the SDK method doesn't work.
        """
        import requests
        import os
        
        # Get Databricks workspace URL and token from environment
        workspace_url = os.environ.get('DATABRICKS_HOST', '')
        token = os.environ.get('DATABRICKS_TOKEN', '')
        
        if not workspace_url or not token:
            logger.warning("Databricks credentials not found. Using mock mode.")
            return self._generate_mock_response(user_prompt)
        
        endpoint_url = f"{workspace_url}/serving-endpoints/{self._endpoint}/invocations"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "max_tokens": self.model_config['max_tokens'],
            "temperature": self.model_config['temperature']
        }
        
        response = requests.post(endpoint_url, headers=headers, json=payload)
        response.raise_for_status()
        
        result = response.json()
        return result['choices'][0]['message']['content'].strip()
    
    def _generate_mock_response(self, user_prompt: str) -> str:
        """Generate a mock response for testing without LLM."""
        # Extract key info from prompt for mock
        import random
        
        templates = [
            "Good morning. Yesterday, your glucose was in target for 70% of the day, and your sleep rating was at least 7. Today, set an alarm to help you go to bed on time.",
            "Good morning. You took 7,890 steps yesterday. That's more than Tuesday! Today, consider taking a walk on your lunch break.",
            "Good morning. You logged your breakfast yesterday. Nice! Today, try logging your lunch, too. Go to the Food section for insights.",
            "Good morning. Yesterday, your glucose was in target for 75% of the day, and you took all your meds. Today, try the next exercise from your program."
        ]
        
        return random.choice(templates)
    
    def generate_insight(
        self,
        daily_rating: str,
        rating_description: str,
        positive_actions: list,
        opportunity: dict,
        greeting: str = "Good morning."
    ) -> Dict[str, Any]:
        """
        Generate a personalized health insight message.
        
        Args:
            daily_rating: The calculated daily rating
            rating_description: Description of the rating
            positive_actions: List of selected positive action dicts
            opportunity: Selected opportunity dict
            greeting: Time-appropriate greeting
            
        Returns:
            Dict containing:
                - message: The generated message text
                - rating: The daily rating
                - rating_description: Rating description
                - word_count: Number of words in generated message
                - success: Whether generation was successful
        """
        # Format prompts
        system_prompt = self._format_system_prompt()
        user_prompt = self._format_user_prompt(
            daily_rating=daily_rating,
            rating_description=rating_description,
            positive_actions=positive_actions,
            opportunity=opportunity,
            greeting=greeting
        )
        
        logger.info("Generating insight for user")
        logger.debug(f"System prompt: {system_prompt[:100]}...")
        logger.debug(f"User prompt: {user_prompt[:200]}...")
        
        try:
            # Generate message (no validation or retries)
            message = self._call_llm(system_prompt, user_prompt)
            
            return {
                'message': message,
                'rating': daily_rating,
                'rating_description': rating_description,
                'word_count': len(message.split()),
                'character_count': len(message),
                'success': True,
                'positive_actions_used': [a['key'] for a in positive_actions],
                'opportunity_used': opportunity.get('key', 'unknown')
            }
            
        except Exception as e:
            logger.error(f"Failed to generate insight: {e}")
            
            # Return fallback message
            fallback = f"{greeting} Kudos for logging in today to work on your health! Browse the Explore section to learn more."
            
            return {
                'message': fallback,
                'rating': daily_rating,
                'rating_description': rating_description,
                'word_count': len(fallback.split()),
                'character_count': len(fallback),
                'success': False,
                'error': str(e),
                'positive_actions_used': [],
                'opportunity_used': 'fallback'
            }


class InsightGeneratorMLflow:
    """
    MLflow-compatible wrapper for the InsightGenerator.
    Use this for Databricks Model Serving deployment.
    """
    
    def __init__(self, config_path: str = "prompts.yml"):
        """Initialize with config path."""
        self.config_path = config_path
        self.generator = None
        self.logic_engine = None
    
    def load_context(self, context):
        """MLflow load_context for initialization."""
        import os
        
        # Config should be in artifacts
        artifacts_path = context.artifacts.get('config_path', self.config_path)
        
        if os.path.exists(artifacts_path):
            self.config_path = artifacts_path
        
        from logic_engine import LogicEngine
        
        self.generator = InsightGenerator(self.config_path)
        self.logic_engine = LogicEngine(self.config_path)
    
    def predict(self, context, model_input):
        """
        MLflow predict method for Model Serving.
        
        Args:
            context: MLflow context
            model_input: DataFrame or dict with patient_id
            
        Returns:
            Generated insight message
        """
        # Handle different input formats
        if hasattr(model_input, 'to_dict'):
            # DataFrame input
            inputs = model_input.to_dict(orient='records')
        elif isinstance(model_input, list):
            inputs = model_input
        else:
            inputs = [model_input]
        
        results = []
        
        for input_row in inputs:
            patient_id = input_row.get('patient_id')
            
            # Get features from Gold table (passed in input or lookup)
            user_context = self._build_user_context(input_row)
            history = self._get_message_history(patient_id)
            
            # Run logic engine
            selected = self.logic_engine.select_content(user_context, history)
            
            # Generate message
            result = self.generator.generate_insight(
                daily_rating=selected.daily_rating,
                rating_description=selected.rating_description,
                positive_actions=selected.positive_actions,
                opportunity=selected.opportunity,
                greeting=selected.greeting
            )
            
            results.append(result)
        
        return results
    
    def _build_user_context(self, input_row: dict):
        """Build UserContext from input row."""
        from logic_engine import UserContext
        from datetime import datetime
        
        return UserContext(
            patient_id=input_row.get('patient_id', ''),
            report_date=datetime.now(),
            
            has_cgm=input_row.get('has_cgm_connected', False),
            has_step_tracker=input_row.get('has_step_tracker', False),
            has_medications=input_row.get('has_medications', False),
            has_weight_goal=input_row.get('has_weight_goal', False),
            weight_goal_type=input_row.get('weight_goal_type'),
            
            tir_pct=input_row.get('tir_pct'),
            glucose_high_pct=input_row.get('glucose_high_pct'),
            glucose_low_pct=input_row.get('glucose_low_pct'),
            
            daily_step_count=input_row.get('daily_step_count'),
            active_minutes=input_row.get('active_minutes'),
            weekly_active_minutes=input_row.get('active_minutes_7d_sum'),
            
            sleep_duration_hours=input_row.get('sleep_duration_hours'),
            sleep_rating=input_row.get('sleep_rating'),
            
            meals_logged_count=input_row.get('unique_meals_logged'),
            any_nutrient_target_met=input_row.get('any_nutrient_target_met', False),
            
            took_all_meds=input_row.get('took_all_meds', False),
            med_adherence_7d_avg=input_row.get('med_adherence_7d_avg'),
        )
    
    def _get_message_history(self, patient_id: str):
        """Get message history (stub - implement with actual lookup)."""
        from logic_engine import MessageHistory
        return MessageHistory(patient_id=patient_id)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def test_generator():
    """Test the insight generator with sample data."""
    generator = InsightGenerator()
    
    # Sample positive actions
    positive_actions = [
        {
            'key': 'glucose_tir_met',
            'category': 'glucose',
            'text': 'Yesterday, your glucose was in target for 70% of the day'
        },
        {
            'key': 'sleep_rating_met',
            'category': 'sleep',
            'text': 'Your sleep rating was at least 7'
        }
    ]
    
    # Sample opportunity
    opportunity = {
        'key': 'sleep_improvement',
        'category': 'sleep',
        'text': 'Set an alarm to help you go to bed on time. This might help you get more hours of sleep tonight.'
    }
    
    result = generator.generate_insight(
        daily_rating="Committed",
        rating_description="You are committed to your healthy habits and seeing results.",
        positive_actions=positive_actions,
        opportunity=opportunity,
        greeting="Good morning."
    )
    
    print("\n" + "="*60)
    print("TEST RESULT")
    print("="*60)
    print(f"Message: {result['message']}")
    print(f"Length: {result['character_count']} characters")
    print(f"Words: {result['word_count']} words")
    print(f"Success: {result['success']}")
    print("="*60)
    
    return result


if __name__ == "__main__":
    test_generator()
