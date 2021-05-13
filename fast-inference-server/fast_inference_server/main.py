import torch
import ujson
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.endpoints import HTTPEndpoint
from starlette.responses import PlainTextResponse


class LinearRegression(torch.nn.Module):
    def __init__(self, inputSize, outputSize):
        super(LinearRegression, self).__init__()
        self.linear = torch.nn.Linear(inputSize, outputSize)

    def forward(self, x):
        out = self.linear(x)
        return out


class HealthCheck(HTTPEndpoint):
    async def get(self, request):
        return PlainTextResponse("0")


class Prediction(HTTPEndpoint):
    async def post(self, request):
        data = await self.receive()
        tensor = torch.Tensor(ujson.loads(data['body']))
        return PlainTextResponse(f"{model(tensor).data.numpy()}")


def load_model():
    model_file = '/opt/app-root/models/model.torch'
    print(f'loading model from: {model_file}', flush=True)
    model = LinearRegression(1, 1)
    model.load_state_dict(torch.load(model_file))
    return model


print(f'loading....', flush=True)
model = load_model()
routes = [Route("/", endpoint=HealthCheck),
          Route("/predict", endpoint=Prediction)]
app = Starlette(debug=True, routes=routes)
