import { GoogleGenerativeAI } from '@google/generative-ai';

const GEMINI_MODEL = 'gemini-3-pro-preview'; 

// --- Tool Adapter ---

function adaptTool(openaiTool: any) {
  // We need to construct the Gemini Tool declaration
  const parameters = JSON.parse(JSON.stringify(openaiTool.parameters));
  
  // Clean up parameters for Gemini
  if (parameters.$schema) delete parameters.$schema;
  if (parameters.additionalProperties !== undefined) delete parameters.additionalProperties;
  
  return {
    declaration: {
      name: openaiTool.name,
      description: openaiTool.description,
      parameters: parameters,
    },
    execute: openaiTool.execute ?? ((args: unknown) => {
      const input = typeof args === 'string' ? args : JSON.stringify(args ?? {});
      return openaiTool.invoke?.(undefined, input);
    })

  };
}

// --- Generic Agent Runner ---

export async function runGeminiAgent<T>(
  apiKey: string,
  systemInstruction: string,
  tools: any[],
  userMessage: string | Array<any>,
  outputToolName: string,
  outputSchema?: any
): Promise<T | null> {
  const genAI = new GoogleGenerativeAI(apiKey);
  const model = genAI.getGenerativeModel({ 
    model: GEMINI_MODEL,
    systemInstruction 
  });

  // 1. Adapt tools
  const adaptedTools = tools.map(adaptTool);
  const toolMap = new Map(adaptedTools.map(t => [t.declaration.name, t.execute]));

  // 2. Add Final Answer Tool (if schema provided, otherwise rely on tools returning data?)
  // The reference passes `finalAnswerSchema` to create a `submit_final_answer` tool.
  
  let allToolDeclarations = [...adaptedTools.map(t => t.declaration)];
  
  if (outputSchema) {
    const finalAnswerToolDeclaration = {
      name: outputToolName,
      description: "Submit the final answer for the user's request. Use this tool when you have gathered all necessary information.",
      parameters: outputSchema
    };
    allToolDeclarations.push(finalAnswerToolDeclaration);
  }

  // 3. Start Chat
  const chat = model.startChat({
    tools: [{ functionDeclarations: allToolDeclarations }]
  });

  let response = await chat.sendMessage(userMessage);
  let result: T | null = null;
  const maxTurns = 15; 
  let turns = 0;

  while (turns < maxTurns) {
    const functionCalls = response.response.functionCalls();
    const call = functionCalls?.[0];
    
    if (call) {
      const { name, args } = call;
      console.log(`🤖 [Gemini] Calling tool: ${name}`);

      if (name === outputToolName) {
        result = args as T;
        break;
      }
      const executor = toolMap.get(name);
      if (executor) {
        try {
          const toolResult = await executor(args);
          // Send result back
          response = await chat.sendMessage([{
            functionResponse: {
              name: name,
              response: { result: toolResult } 
            }
          }]);
        } catch (error: any) {
            console.error(`Error executing tool ${name}:`, error);
            response = await chat.sendMessage([{
                functionResponse: {
                    name: name,
                    response: { error: error.message || "Unknown error" }
                }
            }]);
        }
      } else {
        console.error(`Tool ${name} not found`);
        break;
      }
    } else {
      const text = response.response.text();
      console.log(`🤖 [Gemini] Says: ${text}`);
      
      if (!result && outputSchema) {
         response = await chat.sendMessage(`Please submit the final answer using the ${outputToolName} tool.`);
      } else if (!result && !outputSchema) {
         // If no explicit output tool, maybe the text IS the result? 
         // But for this pattern, we usually want structured output.
         break;
      } else {
          break;
      }
    }
    turns++;
  }

  return result;
}
