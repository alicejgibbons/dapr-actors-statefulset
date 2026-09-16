FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY src/ActorContracts/ActorContracts.csproj src/ActorContracts/
COPY src/ActorHost/ActorHost.csproj src/ActorHost/
RUN dotnet restore src/ActorHost/ActorHost.csproj

COPY src/ActorContracts/ src/ActorContracts/
COPY src/ActorHost/ src/ActorHost/
RUN dotnet publish src/ActorHost/ActorHost.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

ENTRYPOINT ["dotnet", "ActorHost.dll"]
