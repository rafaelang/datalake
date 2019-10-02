No processo de exploração dos dados, algumas anormalidades foram encontradas na estrutura das observações. Aqui registramos esses erros encontrados e o que fizemos para tratá-los.   
Em geral, essas anormalidades estão relacionadas a chaves malformadas nos arquivos json, como espaço em branco e caracteres especiais (@). Essas chaves são problemáticos e devem ser excluídas ou atualizadas porque algumas estruturas/serviços, como o Athena, não conseguem fazer consultas em dados que contenham-nas. As queries não são executadas, e erros são acusados.


**Dados de CheckoutOrder / FulfillmentOrder**

  - Campo `productCategories` dentro de `Items`: as chaves são identificadores e os valores são os nomes da categoria. *(Corrigido: criação de um objeto com as chaves: id e name)*;

  - Qualquer campo que contenha a string `attachment`: em todos os casos, as chaves são mal formadas, pois possuem ou espaço em branco ou caracteres especiais (ex: arroba). *(Corrigido: todas as chaves que possuem o attachment foram excluídas)*;

  - Campo `assemblyOptions` dentro de `ItemMetadata`: assim como nos campos com `attachment`, neste existe um campo interno chamado de `inputValue` que também contém chaves mal formadas, ou sejam, com caracteres especiais; *(Corrigido: assemblyOptions foi excluída das observações).*

  - Campo `rateAndBenefitsIdentifiers` dentro de `ratesandbenefitsdata`, contém o campo `matchedParameters`, que assim como os anteriores, possui chaves mal formadas com caracteres especiais. *(Corrigido: matchedParameters foi excluída das observações).*

Outros erros foram encontrados depois de estruturar e particionar os arquivos:

  - Campo `CustomData` tinha diversos campos, não padronizados (semelhante ao caso de attachements). Como eram campos sem importância de valor, deletamos essa coluna da estrutura final de *consumable_tables*

  - Campo `CommercialConditionData` da mesma forma possui diferentes tipos de estrutura. _Também foi apagado_ da estrutura final.


Todos esses erros foram corrigidos tanto no lambda que estrutura os arquivos quanto no script de particionamento/estruturação de dados de migração.