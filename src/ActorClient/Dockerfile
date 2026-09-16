FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY src/ActorContracts/ActorContracts.csproj src/ActorContracts/
COPY src/ActorClient/ActorClient.csproj src/ActorClient/
RUN dotnet restore src/ActorClient/ActorClient.csproj

COPY src/ActorContracts/ src/ActorContracts/
COPY src/ActorClient/ src/ActorClient/
RUN dotnet publish src/ActorClient/ActorClient.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

ENTRYPOINT ["dotnet", "ActorClient.dll"]
