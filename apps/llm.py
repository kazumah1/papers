from openai import OpenAI
class LLMClient():
    def __init__(self):
        ...

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
    ...
