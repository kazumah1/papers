from openai import OpenAI
from ollama import chat, ChatResponse
from transformers import pipeline
from dotenv import load_dotenv

class LLMClient():
    def __init__(self):
        load_dotenv()

    def summarize(self, text):
        ...

    def caption(self, image, text=""):
        ...


class OpenAIClient(LLMClient):
    def __init__(self):
        super().__init__()
        self.client = OpenAI()
    
    def summarize(self, text):
        response = self.client.responses.create(
                model="gpt-5",
                reasoning={"effort":"low"},
                instructions="You are an academic scholar and researcher with years of professional research experience. You are profficient in all fields, particularly in STEM. You will be given the text contents of a research paper that you are tasked with summarizing. The text has been extracted from an HTML page, and thus, some figures and images may not be present, although their captions will be. The summary should include the necessary aspects of the research paper, such as the problem they are trying to solve, the methods that they used to solve it, and the results. This summary should include all of the relevant and important concepts in the paper. The summary should include enough information that the reader can effectively discuss and apply the methods and findings of the paper without having to read the entire original paper.",
                input=f"Paper Text: {text}"
        )
        print(response.output_text)
        return response.output_text

class HFClient(LLMClient):
    def __init__(self):
        super().__init__()
        self.client = pipeline("text-generation", model="deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B", trust_remote_code=True)

    
    def summarize(self, text):
        messages = [
                {"role":"system", "content": "You are an academic scholar and researcher with years of professional research experience. You are profficient in all fields, particularly in STEM. You will be given the text contents of a research paper that you are tasked with summarizing. The text has been extracted from an HTML page, and thus, some figures and images may not be present, although their captions will be. The summary should include the necessary aspects of the research paper, such as the problem they are trying to solve, the methods that they used to solve it, and the results. This summary should include all of the relevant and important concepts in the paper. The summary should include enough information that the reader can effectively discuss and apply the methods and findings of the paper without having to read the entire original paper. Your output should be only the summary, and you should not do any thinking - just return the summary immediately."},
                {"role":"user", "content": f"Paper Text: {text}"}
        ]
        response = self.client(messages)
        print(response[0].get('generated_text', [])[-1])
        return response

class OllamaClient(LLMClient):
    def __init__(self):
        super().__init__()
    
    def summarize(self, text):
        print("text:", text)
        messages = [
            {"role":"system", "content": "You are an academic scholar and researcher with years of professional research experience. You are profficient in all fields, particularly in STEM. You will be given the text contents of a research paper that you are tasked with summarizing. The text has been extracted from an HTML page, and thus, some figures and images may not be present, although their captions will be. The summary should include the necessary aspects of the research paper, such as the problem they are trying to solve, the methods that they used to solve it, and the results. This summary should include all of the relevant and important concepts in the paper. The summary should include enough information that the reader can effectively discuss and apply the methods and findings of the paper without having to read the entire original paper. Your output should be only the summary, and nothing else."},
            {"role":"user", "content": f"Paper Text: {text}"}
        ]
        response: ChatResponse = chat(model="qwen:latest", messages=messages)
        print(response['message']['content'])
        return response.message.content
